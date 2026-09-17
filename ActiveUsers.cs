using Azure;
using Azure.Identity;
using Azure.Monitor.Query.Logs;
using Azure.Monitor.Query.Logs.Models;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Microsoft.AspNetCore.Http;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Parquet;
using Parquet.Schema;


namespace GCStats
{
    public class ActiveUsers
    {
        private readonly ILogger<ActiveUsers> _logger;
        private readonly IConfiguration _config;

        public ActiveUsers(ILogger<ActiveUsers> logger, IConfiguration config)
        {
            _logger = logger;
            _config = config;
        }

        
        [Function("ActiveUsers")]
        [QueueOutput("process-active-users", Connection = "AzureWebJobsStorage")]
        public async Task<string> Run([TimerTrigger(Globals.TimerStartTime)] TimerInfo timer)
        //[Function("ActiveUsers")]
        //[QueueOutput("process-active-users", Connection = "AzureWebJobsStorage")]
        //public async Task<string> Run([HttpTrigger(AuthorizationLevel.Function, "get", "post")] HttpRequest req)
        {
            _logger.LogInformation($"Timer trigger function executed at: {DateTime.UtcNow}");

            var blobName = await StreamActiveUsersToBlobAsync();

            _logger.LogInformation($"BlobName: {blobName}");

            return blobName;
        }

        public async Task<string> StreamActiveUsersToBlobAsync()
        {
            try
            {
                var workspaceId = Auth.GetAppSetting("workspaceId", _logger, _config);
                var storageAccountUrl = Auth.GetAppSetting("storageAccountUrl", _logger, _config);
                var isLocal = Auth.GetAppSetting("isLocal", _logger, _config, false);

                var snapshotDate = DateTime.UtcNow.Date;
                var blobName = $"{Users.ActiveUsersContainerName}-{DateTime.UtcNow.ToString(Globals.BlobDateFormat)}.parquet";

                var logsQueryClient = await Auth.GetLogsQueryClient(_logger);
                var blobServiceClient = new BlobServiceClient(new Uri(storageAccountUrl), isLocal == "true" ? new AzureCliCredential() : new DefaultAzureCredential());
                
                var containerClient = blobServiceClient.GetBlobContainerClient(Users.ActiveUsersContainerName);
                await containerClient.CreateIfNotExistsAsync(PublicAccessType.None);
                var blobClient = containerClient.GetBlobClient(blobName);

                string query = @"
                  SigninLogs | where TimeGenerated >= ago(24h)
                    | where UserPrincipalName != UserId 
                    | where ResourceDisplayName == 'Office 365 SharePoint Online' or ResourceDisplayName contains 'Microsoft Teams' 
                    | where AppDisplayName in ('Microsoft Teams', 'Office 365 SharePoint Online')
                    | summarize LastCall = max(TimeGenerated) by UserDisplayName, UserPrincipalName, UserId, UserType, ResourceDisplayName, AppDisplayName 
                    | distinct UserId, UserDisplayName, UserPrincipalName, ResourceDisplayName, AppDisplayName, LastCall 
                    | order by LastCall asc
                ";

                Response<LogsQueryResult> response = await logsQueryClient.QueryWorkspaceAsync(
                    workspaceId: workspaceId,
                    query: query,
                    timeRange: new LogsQueryTimeRange(TimeSpan.FromHours(24))
                );

                var idField = new DataField<string>("Id");
                var snapshotDateField = new DataField<DateTime>("SnapshotDate");
                var schema = new ParquetSchema(idField, snapshotDateField);

                var parquetOptions = new ParquetOptions
                {
                    CompressionMethod = CompressionMethod.Snappy
                };

                using var blobStream = await blobClient.OpenWriteAsync(overwrite: true);
                await using var parquetWriter = await ParquetWriter.CreateAsync(schema, blobStream, parquetOptions);

                var idBuffer = new List<string>(Globals.RowGroupBatchSize);
                var snapshotDateBuffer = new List<DateTime>(Globals.RowGroupBatchSize);

                async Task FlushBatchAsync()
                {
                    if (idBuffer.Count == 0)
                        return;

                    using var groupWriter = parquetWriter.CreateRowGroup();

                    await groupWriter.WriteAsync(idField, idBuffer);
                    await groupWriter.WriteAsync<DateTime>(snapshotDateField, snapshotDateBuffer.ToArray().AsMemory());

                    idBuffer.Clear();
                    snapshotDateBuffer.Clear();
                }
                
                foreach (var row in response.Value.Table.Rows)
                {
                    idBuffer.Add(row["UserId"]?.ToString() ?? string.Empty);
                    snapshotDateBuffer.Add(snapshotDate);

                    if (idBuffer.Count >= Globals.RowGroupBatchSize)
                    {
                        FlushBatchAsync().GetAwaiter().GetResult();
                    }
                }

                await FlushBatchAsync();
                await parquetWriter.DisposeAsync();

                _logger.LogInformation($"Saved {response.Value.Table.Rows.Count} active users to blob: {blobName}");

                return blobName;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex.Message);
                throw;
            }
        }
    }
}

