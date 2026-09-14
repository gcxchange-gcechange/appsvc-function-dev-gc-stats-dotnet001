using Azure.Identity;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Sas;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System.Data;
using System.Text.Json;

namespace GCStats
{
    public class ProcessTotalCommunities
    {
        private readonly ILogger<ProcessTotalCommunities> _logger;
        private readonly IConfiguration _config;

        public ProcessTotalCommunities(ILogger<ProcessTotalCommunities> logger, IConfiguration config)
        {
            _logger = logger;
            _config = config;
        }

        [Function("ProcessTotalCommunities")]
        public async Task Run([QueueTrigger("process-total-communities", Connection = "AzureWebJobsStorage")] string blobName)
        {
            _logger.LogInformation("Received blobName: {blobName}", blobName);

            try
            {
                var storageAccountUrl = Globals.GetAppSetting("storageAccountUrl", _logger, _config);
                var isLocal = Globals.GetAppSetting("isLocal", _logger, _config, false);
                var credential = isLocal == "true" ? new AzureCliCredential() : (Azure.Core.TokenCredential)new DefaultAzureCredential();

                var blobServiceClient = new BlobServiceClient(new Uri(storageAccountUrl), credential);

                var date = blobName.Replace("communities-", string.Empty).Replace(".parquet", string.Empty);
                var ownersBlobName = $"community-owners-{date}.parquet";
                var membersBlobName = $"community-members-{date}.parquet";

                var communitiesContainerClient = blobServiceClient.GetBlobContainerClient(Communities.TotalCommunitiesContainerName);
                var ownersContainerClient = blobServiceClient.GetBlobContainerClient(Communities.CommunityOwnersContainerName);
                var membersContainerClient = blobServiceClient.GetBlobContainerClient(Communities.CommunityMembersContainerName);

                var communitiesBlobClient = communitiesContainerClient.GetBlobClient(blobName);
                var ownersBlobClient = ownersContainerClient.GetBlobClient(ownersBlobName);
                var membersBlobClient = membersContainerClient.GetBlobClient(membersBlobName);

                if (!await communitiesBlobClient.ExistsAsync())
                    throw new FileNotFoundException($"Blob {blobName} not found in container {Communities.TotalCommunitiesContainerName}");

                if (!await ownersBlobClient.ExistsAsync())
                    throw new FileNotFoundException($"Blob {ownersBlobName} not found in container {Communities.CommunityOwnersContainerName}");

                if (!await membersBlobClient.ExistsAsync())
                    throw new FileNotFoundException($"Blob {membersBlobName} not found in container {Communities.CommunityMembersContainerName}");

                var delegationKey = await blobServiceClient.GetUserDelegationKeyAsync(
                    new BlobGetUserDelegationKeyOptions(DateTimeOffset.UtcNow.AddMinutes(15))
                    {
                        StartsOn = DateTimeOffset.UtcNow.AddMinutes(-5)
                    }
                );

                using var sqlConnection = await Auth.GetSqlConnection(_logger, _config);

                await CopyParquetIntoTableAsync(sqlConnection, "dbo.TotalCommunities", communitiesContainerClient, communitiesBlobClient, delegationKey.Value, blobServiceClient.AccountName);
                _logger.LogInformation("Successfully uploaded communities from {blobName} to dbo.TotalCommunities", blobName);

                await CopyParquetIntoTableAsync(sqlConnection, "dbo.CommunityOwners", ownersContainerClient, ownersBlobClient, delegationKey.Value, blobServiceClient.AccountName);
                _logger.LogInformation("Successfully uploaded owners from {blobName} to dbo.CommunityOwners", ownersBlobName);

                await CopyParquetIntoTableAsync(sqlConnection, "dbo.CommunityMembers", membersContainerClient, membersBlobClient, delegationKey.Value, blobServiceClient.AccountName);
                _logger.LogInformation("Successfully uploaded members from {blobName} to dbo.CommunityMembers", membersBlobName);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex.Message);
                throw;
            }

            _logger.LogInformation("Finished processing {blobName}", blobName);
        }

        private static async Task CopyParquetIntoTableAsync(SqlConnection sqlConnection, string destinationTable, BlobContainerClient containerClient, BlobClient blobClient, UserDelegationKey delegationKey, string accountName)
        {
            var sasBuilder = new BlobSasBuilder
            {
                BlobContainerName = containerClient.Name,
                BlobName = blobClient.Name,
                Resource = "b",
                StartsOn = DateTimeOffset.UtcNow.AddMinutes(-5),
                ExpiresOn = DateTimeOffset.UtcNow.AddMinutes(15)
            };
            sasBuilder.SetPermissions(BlobSasPermissions.Read);

            var sasToken = sasBuilder.ToSasQueryParameters(delegationKey, accountName).ToString();

            var blobUrl = blobClient.Uri.ToString().Replace("'", "''");
            var sasSecret = sasToken.Replace("'", "''");

            using var copyCmd = new SqlCommand(
                $"""
                 COPY INTO {destinationTable}
                 FROM '{blobUrl}'
                 WITH (
                     FILE_TYPE = 'PARQUET',
                     CREDENTIAL = (IDENTITY = 'Shared Access Signature', SECRET = '{sasSecret}')
                 );
                 """,
                sqlConnection
            );

            copyCmd.CommandTimeout = 0;
            await copyCmd.ExecuteNonQueryAsync();
        }
    }
}