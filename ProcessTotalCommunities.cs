using Azure.Identity;
using Azure.Storage.Blobs;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

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
                var storageAccountUrl = Auth.GetAppSetting("storageAccountUrl", _logger, _config);
                var isLocal = Auth.GetAppSetting("isLocal", _logger, _config, false);
                var credential = isLocal == "true" ? new AzureCliCredential() : (Azure.Core.TokenCredential)new DefaultAzureCredential();

                var blobServiceClient = new BlobServiceClient(new Uri(storageAccountUrl), credential);

                var date = blobName.Replace($"{Communities.TotalCommunitiesContainerName}-", string.Empty).Replace(".parquet", string.Empty);
                var ownersBlobName = $"{Communities.CommunityOwnersContainerName}-{date}.parquet";
                var membersBlobName = $"{Communities.CommunityMembersContainerName}-{date}.parquet";

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

                using var sqlConnection = await Auth.BuildSqlConnection(_logger, _config);

                await CopyParquetIntoTableAsync(sqlConnection, "dbo.Communities", communitiesBlobClient);
                _logger.LogInformation($"Successfully uploaded communities from {blobName} to dbo.Communities");

                await CopyParquetIntoTableAsync(sqlConnection, "dbo.CommunityOwners", ownersBlobClient);
                _logger.LogInformation($"Successfully uploaded owners from {ownersBlobName} to dbo.CommunityOwners");

                await CopyParquetIntoTableAsync(sqlConnection, "dbo.CommunityMembers", membersBlobClient);
                _logger.LogInformation($"Successfully uploaded members from {membersBlobName} to dbo.CommunityMembers");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Failed to process communities blob {blobName}");

                throw;
            }

            _logger.LogInformation($"Finished processing {blobName}");
        }

        private static async Task CopyParquetIntoTableAsync(SqlConnection sqlConnection, string destinationTable, BlobClient blobClient)
        {
            var blobUrl = blobClient.Uri.ToString().Replace("'", "''");

            using var copyCmd = new SqlCommand(
                $"""
                 COPY INTO {destinationTable}
                 FROM '{blobUrl}'
                 WITH (
                     FILE_TYPE = 'PARQUET',
                     CREDENTIAL = (IDENTITY = 'Workspace Identity')
                 );
                 """,
                sqlConnection
            );

            copyCmd.CommandTimeout = 0;
            await copyCmd.ExecuteNonQueryAsync();
        }
    }
}