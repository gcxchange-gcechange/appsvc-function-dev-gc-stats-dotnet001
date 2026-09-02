using Azure.Core;
using Azure.Identity;
using Azure.Storage.Blobs;
using Azure.Storage.Sas;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System.Data;
using System.Text.Json;

namespace GCStats
{
    public class ProcessTotalUsers
    {
        private readonly ILogger<ProcessTotalUsers> _logger;
        private readonly IConfiguration _config;

        public ProcessTotalUsers(ILogger<ProcessTotalUsers> logger, IConfiguration config)
        {
            _logger = logger;
            _config = config;
        }

        [Function("ProcessTotalUsers")]
        public async Task Run([QueueTrigger("process-total-users", Connection = "AzureWebJobsStorage")] string blobName)
        {
            _logger.LogInformation("Received blobName: {blobName}", blobName);

            try
            {
                var storageAccountUrl = Globals.GetAppSetting("storageAccountUrl", _logger, _config);
                var isLocal = Globals.GetAppSetting("isLocal", _logger, _config, false);
                var credential = isLocal == "true" ? new AzureCliCredential() : (Azure.Core.TokenCredential)new DefaultAzureCredential();

                var blobServiceClient = new BlobServiceClient(new Uri(storageAccountUrl), credential);
                var containerClient = blobServiceClient.GetBlobContainerClient(Users.TotalUsersContainerName);
                var blobClient = containerClient.GetBlobClient(blobName);

                if (!await blobClient.ExistsAsync())
                    throw new FileNotFoundException($"Blob {blobName} not found in container {Users.TotalUsersContainerName}");

                var snapshotDate = Globals.GetDateFromBlob(blobName, _logger);

                var delegationKey = await blobServiceClient.GetUserDelegationKeyAsync(
                    new Azure.Storage.Blobs.Models.BlobGetUserDelegationKeyOptions(DateTimeOffset.UtcNow.AddMinutes(15))
                    {
                        StartsOn = DateTimeOffset.UtcNow.AddMinutes(-5)
                    }
                 );

                var sasBuilder = new BlobSasBuilder
                {
                    BlobContainerName = containerClient.Name,
                    BlobName = blobClient.Name,
                    Resource = "b",
                    StartsOn = DateTimeOffset.UtcNow.AddMinutes(-5),
                    ExpiresOn = DateTimeOffset.UtcNow.AddMinutes(15)
                };
                sasBuilder.SetPermissions(BlobSasPermissions.Read);

                var sasToken = sasBuilder.ToSasQueryParameters(delegationKey.Value, blobServiceClient.AccountName).ToString();

                var blobUrl = blobClient.Uri.ToString().Replace("'", "''");
                var sasSecret = sasToken.Replace("'", "''");

                using var sqlConnection = await Auth.GetSqlConnection(_logger, _config);
                using var transaction = sqlConnection.BeginTransaction();

                try
                {
                    using (var createCmd = new SqlCommand(
                        """
                        CREATE TABLE #StagingUsers (
                            Id VARCHAR(36),
                            Mail VARCHAR(320)
                        ) WITH (DISTRIBUTION = ROUND_ROBIN);
                        """,
                        sqlConnection, transaction))
                    {
                        await createCmd.ExecuteNonQueryAsync();
                    }

                    using (var copyCmd = new SqlCommand(
                        $"""
                         COPY INTO #StagingUsers
                         FROM '{blobUrl}'
                         WITH (
                             FILE_TYPE = 'PARQUET',
                             CREDENTIAL = (IDENTITY = 'Shared Access Signature', SECRET = '{sasSecret}')
                         );
                         """,
                        sqlConnection, transaction))
                    {
                        copyCmd.CommandTimeout = 0;
                        await copyCmd.ExecuteNonQueryAsync();
                    }

                    int stagedCount;
                    using (var countCmd = new SqlCommand("SELECT COUNT(*) FROM #StagingUsers;", sqlConnection, transaction))
                    {
                        stagedCount = (int)await countCmd.ExecuteScalarAsync();
                    }

                    if (stagedCount == 0)
                    {
                        throw new DataException("No users to upload");
                    }

                    _logger.LogInformation("Copied {count} users from blob {blobName} into staging", stagedCount, blobName);

                    using (var insertCmd = new SqlCommand(
                        """
                        INSERT INTO dbo.TotalUsers (Id, Mail, SnapshotDate)
                        SELECT Id, Mail, @SnapshotDate
                        FROM #StagingUsers;
                        """,
                        sqlConnection, transaction))
                    {
                        insertCmd.Parameters.AddWithValue("@SnapshotDate", snapshotDate);
                        await insertCmd.ExecuteNonQueryAsync();
                    }

                    transaction.Commit();

                    _logger.LogInformation("Successfully uploaded {count} users to dbo.TotalUsers", stagedCount);
                }
                catch
                {
                    transaction.Rollback();
                    throw;
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex.Message);
                throw;
            }

            _logger.LogInformation("Finished processing {blobName}", blobName);
        }
    }
}
