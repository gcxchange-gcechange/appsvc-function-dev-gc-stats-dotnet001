using Azure.Identity;
using Azure.Storage.Blobs;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

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
                var storageAccountUrl = Auth.GetAppSetting("storageAccountUrl", _logger, _config);
                var isLocal = Auth.GetAppSetting("isLocal", _logger, _config, false);
                var credential = isLocal == "true" ? new AzureCliCredential() : (Azure.Core.TokenCredential)new DefaultAzureCredential();

                var blobServiceClient = new BlobServiceClient(new Uri(storageAccountUrl), credential);
                var containerClient = blobServiceClient.GetBlobContainerClient(Users.TotalUsersContainerName);
                var blobClient = containerClient.GetBlobClient(blobName);

                if (!await blobClient.ExistsAsync())
                    throw new FileNotFoundException($"Blob {blobName} not found in container {Users.TotalUsersContainerName}");

                using var sqlConnection = await Auth.BuildSqlConnection(_logger, _config);

                var blobUrl = blobClient.Uri.ToString().Replace("'", "''");

                using var copyCmd = new SqlCommand(
                    $"""
                     COPY INTO dbo.Users
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

                _logger.LogInformation($"Successfully uploaded users from {blobName} to dbo.Users");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Failed to process total users blob {blobName}");

                throw;
            }

            _logger.LogInformation($"Finished processing {blobName}");
        }
    }
}