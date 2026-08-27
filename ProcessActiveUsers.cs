using Azure.Identity;
using Azure.Storage.Blobs;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System.Data;
using System.Text.Json;

namespace GCStats
{
    public class ProcessActiveUsers
    {
        private readonly ILogger<ProcessActiveUsers> _logger;
        private readonly IConfiguration _config;

        public ProcessActiveUsers(ILogger<ProcessActiveUsers> logger, IConfiguration config)
        {
            _logger = logger;
            _config = config;
        }

        [Function("ProcessActiveUsers")]
        public async Task Run([QueueTrigger("process-active-users", Connection = "AzureWebJobsStorage")] string blobName)
        {
            _logger.LogInformation("Received blobName: {blobName}", blobName);

            try
            {
                var storageAccountUrl = Globals.GetAppSetting("storageAccountUrl", _logger, _config);
                var isLocal = Globals.GetAppSetting("isLocal", _logger, _config, false);

                var blobServiceClient = new BlobServiceClient(new Uri(storageAccountUrl), isLocal == "true" ? new AzureCliCredential() : new DefaultAzureCredential());
                var containerClient = blobServiceClient.GetBlobContainerClient(Users.ActiveUsersContainerName);
                var blobClient = containerClient.GetBlobClient(blobName);

                var response = await blobClient.DownloadContentAsync();
                var users = JsonSerializer.Deserialize<List<UserRecord>>(response.Value.Content.ToString(), new JsonSerializerOptions { PropertyNameCaseInsensitive = true });

                if (users != null && users.Count > 0)
                {
                    _logger.LogInformation("Processing {count} users from blob: {blobName}", users.Count, blobName);

                    var dataTable = new DataTable();

                    dataTable.Columns.Add("Id", typeof(string));
                    dataTable.Columns.Add("Mail", typeof(string));
                    dataTable.Columns.Add("SnapshotDate", typeof(DateTime));

                    var snapshotDate = Globals.GetDateFromBlob(blobName, _logger);

                    foreach (var user in users)
                    {
                        dataTable.Rows.Add(user.Id, user.Mail, snapshotDate);
                    }

                    using var sqlConnection = await Auth.GetSqlConnection(_logger, _config);

                    using var bulkCopy = new SqlBulkCopy(sqlConnection);
                    bulkCopy.DestinationTableName = "dbo.ActiveUsers";
                    bulkCopy.BatchSize = 50000;
                    bulkCopy.BulkCopyTimeout = 0;

                    bulkCopy.ColumnMappings.Add("Id", "Id");
                    bulkCopy.ColumnMappings.Add("Mail", "Mail");
                    bulkCopy.ColumnMappings.Add("SnapshotDate", "SnapshotDate");

                    await bulkCopy.WriteToServerAsync(dataTable);

                    _logger.LogInformation("Successfully uploaded {count} users to dbo.ActiveUsers", users.Count);
                }
                else
                {
                    throw new DataException("No users to upload");
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
