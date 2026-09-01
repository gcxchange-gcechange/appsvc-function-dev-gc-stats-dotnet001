using Azure;
using Azure.Identity;
using Azure.Monitor.Query.Logs;
using Azure.Monitor.Query.Logs.Models;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System.Text.Json;
using Microsoft.AspNetCore.Http;

namespace GCStats
{
    public record ActiveUserRecord(string Id, DateTime ActivityDateTime);

    public class ActiveUsers
    {
        private readonly ILogger<ActiveUsers> _logger;
        private readonly IConfiguration _config;

        public ActiveUsers(ILogger<ActiveUsers> logger, IConfiguration config)
        {
            _logger = logger;
            _config = config;
        }

        
        //[Function("ActiveUsers")]
        //[QueueOutput("process-active-users", Connection = "AzureWebJobsStorage")]
        //public async Task<string> Run([TimerTrigger(Globals.TimerStartTime)] TimerInfo timer)
        [Function("ActiveUsers")]
        [QueueOutput("process-active-users", Connection = "AzureWebJobsStorage")]
        public async Task<string> Run([HttpTrigger(AuthorizationLevel.Function, "get", "post")] HttpRequest req)
        {
            _logger.LogInformation($"Timer trigger function executed at: {DateTime.UtcNow}");

            var blobName = await GetActiveUsers(_logger);

            _logger.LogInformation($"BlobName: {blobName}");

            return blobName;
        }

        public async Task<string> GetActiveUsers(ILogger log)
        {
            string workspaceId = Globals.GetAppSetting("workspaceId", log, _config);
            var storageAccountUrl = Globals.GetAppSetting("storageAccountUrl", log, _config);
            var isLocal = Globals.GetAppSetting("isLocal", log, _config, false);

            var client = await Auth.LogsAuth(log);

            var blobServiceClient = new BlobServiceClient(new Uri(storageAccountUrl), isLocal == "true" ? new AzureCliCredential() : new DefaultAzureCredential());

            try
            {
                string blobName = $"{Users.ActiveUsersContainerName}-{DateTime.UtcNow.ToString(Globals.BlobDateFormat)}.json";
                var containerClient = blobServiceClient.GetBlobContainerClient(Users.ActiveUsersContainerName);
                await containerClient.CreateIfNotExistsAsync(PublicAccessType.None);
                var blobClient = containerClient.GetBlobClient(blobName);

                // Get active users from the logs using the LogsQueryClient
                string query = @"
                  SigninLogs | where TimeGenerated >= ago(24h)
                    | where UserPrincipalName != UserId 
                    | where ResourceDisplayName == 'Office 365 SharePoint Online' or ResourceDisplayName contains 'Microsoft Teams' 
                    | where AppDisplayName in ('Microsoft Teams', 'Office 365 SharePoint Online')
                    | summarize LastCall = max(TimeGenerated) by UserDisplayName, UserPrincipalName, UserId, UserType, ResourceDisplayName, AppDisplayName 
                    | distinct UserId, UserDisplayName, UserPrincipalName, ResourceDisplayName, AppDisplayName, LastCall 
                    | order by LastCall asc
                ";

                Response<LogsQueryResult> response = await client.QueryWorkspaceAsync(
                    workspaceId: workspaceId,
                    query: query,
                    timeRange: new LogsQueryTimeRange(TimeSpan.FromHours(24))
                );

                var activeUsers = new List<ActiveUserRecord>();
                foreach (var row in response.Value.Table.Rows)
                {
                    var userId = row["UserId"]?.ToString() ?? "";
                    var activityDateTime = row["LastCall"] is DateTime dateTime ? dateTime : DateTime.UtcNow.AddDays(-1);

                    activeUsers.Add(new ActiveUserRecord(userId, activityDateTime));
                }

                log.LogInformation(activeUsers.Count + " active users retrieved.");

                using var blobStream = await blobClient.OpenWriteAsync(overwrite: true);

                await JsonSerializer.SerializeAsync(blobStream, activeUsers, Globals.JsonOptions);
                await blobStream.FlushAsync();

                log.LogInformation($"Saved {activeUsers.Count} active users to blob: {blobName}");

                return blobName;
            }
            catch (Exception ex)
            {
                log.LogError(ex.Message);
                throw;
            }
        }
    }
}

