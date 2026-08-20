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

                var blobServiceClient = new BlobServiceClient(new Uri(storageAccountUrl), isLocal == "true" ? new AzureCliCredential() : new DefaultAzureCredential());
                var containerClient = blobServiceClient.GetBlobContainerClient(Users.TotalUsersContainerName);
                var blobClient = containerClient.GetBlobClient(blobName);

                var response = await blobClient.DownloadContentAsync();
                var users = JsonSerializer.Deserialize<List<UserRecord>>(response.Value.Content.ToString(), new JsonSerializerOptions { PropertyNameCaseInsensitive = true });

                if (users != null && users.Count > 0)
                {
                    var dataTable = new DataTable();

                    dataTable.Columns.Add("Id", typeof(string));
                    dataTable.Columns.Add("Mail", typeof(string));
                    dataTable.Columns.Add("SnapshotDate", typeof(string));

                    var splitBlobName = blobName.Split('-').Skip(1);
                    var snapshotDate = String.Join("-", splitBlobName);

                    foreach (var user in users)
                    {
                        dataTable.Rows.Add(user.Id, user.Mail, String.Join("-", snapshotDate));
                    }

                    var warehouseServer = Globals.GetAppSetting("warehouseServer", _logger, _config);
                    var warehouseDatabase = Globals.GetAppSetting("warehouseDatabase", _logger, _config);

                    var connectionString =
                    $"Server={warehouseServer};" +
                    $"Database={warehouseDatabase};" +
                    "Authentication=Active Directory Default;" +
                    "Encrypt=True;";

                    await using var connection = new SqlConnection(connectionString);

                    await connection.OpenAsync();

                    using var bulkCopy = new SqlBulkCopy(connection);
                    bulkCopy.DestinationTableName = "dbo.TotalUsers";
                    bulkCopy.BatchSize = 50000;
                    bulkCopy.BulkCopyTimeout = 0;

                    bulkCopy.ColumnMappings.Add("Id", "Id");
                    bulkCopy.ColumnMappings.Add("Mail", "Mail");
                    bulkCopy.ColumnMappings.Add("SnapshotDate", "SnapshotDate");

                    await bulkCopy.WriteToServerAsync(dataTable);

                    _logger.LogInformation("Successfully uploaded {count} users to dbo.TotalUsers", users.Count);
                }
                else
                {
                    throw new DataException("No users to upload");
                }
            }
            catch (Exception ex) 
            {
                _logger.LogError(ex.Message);
            }

            _logger.LogInformation("Finished processing {blobName}", blobName);
        }
    }
}
