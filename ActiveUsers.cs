using Azure;
using Azure.Monitor.Query.Logs;
using Azure.Monitor.Query.Logs.Models;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Parquet;
using Parquet.Schema;


namespace GCStats
{
    public class ActiveUsers : IJob
    {
        private readonly ILogger<ActiveUsers> _logger;
        private readonly IConfiguration _config;
        private readonly JobQueue _queue;

        public string Name => "ActiveUsers";

        public ActiveUsers(ILogger<ActiveUsers> logger, IConfiguration config, JobQueue queue)
        {
            _logger = logger;
            _config = config;
            _queue = queue;
        }

        [Function("Timer_ActiveUsers")]
        public Task Schedule([TimerTrigger(Globals.TimerStartTime)] TimerInfo timer) => _queue.EnqueueJobAsync(Name);

        public async Task RunAsync(CancellationToken ct)
        {
            _logger.LogInformation($"{Name} executed at: {DateTime.UtcNow}");

            var blobName = await StreamActiveUsersToBlobAsync();

            _logger.LogInformation($"BlobName: {blobName}");

            await _queue.SendAsync("process-active-users", blobName);
        }

        public async Task<string> StreamActiveUsersToBlobAsync()
        {
            try
            {
                var snapshotDate = DateTime.UtcNow.Date;
                var blobName = $"{Users.ActiveUsersContainerName}-{DateTime.UtcNow.ToString(Globals.BlobDateFormat)}.parquet";

                var blobClient = await Auth.GetBlobClient(Users.ActiveUsersContainerName, blobName, _logger, _config);
                var logsQueryClient = Auth.GetLogsQueryClient(_logger);

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
                    workspaceId: Auth.GetAppSetting("workspaceId", _logger, _config),
                    query: query,
                    timeRange: new LogsQueryTimeRange(TimeSpan.FromHours(24))
                );

                var userIdField = new DataField<string>("UserId");
                var snapshotDateField = new DataField<DateTime>("SnapshotDate");
                var schema = new ParquetSchema(userIdField, snapshotDateField);

                using var blobStream = await blobClient.OpenWriteAsync(overwrite: true);
                await using var parquetWriter = await ParquetWriter.CreateAsync(schema, blobStream, Globals.ParquetOptions);

                var userIdBuffer = new List<string>(Globals.RowGroupBatchSize);
                var snapshotDateBuffer = new List<DateTime>(Globals.RowGroupBatchSize);

                async Task FlushBatchAsync()
                {
                    if (userIdBuffer.Count == 0)
                        return;

                    using var groupWriter = parquetWriter.CreateRowGroup();

                    await groupWriter.WriteAsync(userIdField, userIdBuffer);
                    await groupWriter.WriteAsync<DateTime>(snapshotDateField, snapshotDateBuffer.ToArray().AsMemory());

                    userIdBuffer.Clear();
                    snapshotDateBuffer.Clear();
                }
                
                foreach (var row in response.Value.Table.Rows)
                {
                    userIdBuffer.Add(row["UserId"]?.ToString() ?? string.Empty);
                    snapshotDateBuffer.Add(snapshotDate);

                    if (userIdBuffer.Count >= Globals.RowGroupBatchSize)
                    {
                        await FlushBatchAsync();
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

