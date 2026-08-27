using Azure;
using Azure.Identity;
using Azure.Monitor.Query.Logs;
using Azure.Monitor.Query.Logs.Models;
using Azure.Security.KeyVault.Secrets;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System.Text.Json;


namespace GCStats
{

    public record ActiveUsersRecord(
     string Id,
     string Email
   );

    public class ActiveUsers
    {
        private readonly ILogger<ActiveUsers> _logger;
        private readonly IConfiguration _config;

        public ActiveUsers(ILogger<ActiveUsers> logger, IConfiguration config)
        {
            _logger = logger;
            _config = config;
        }



        public async Task<List<ActiveUsersRecord>> GetActiveUsers(ILogger log)
        {
            string tenantId = Globals.GetAppSetting("tenantId", log, _config);
            string clientId = Globals.GetAppSetting("clientId", log, _config);
            string secretName = Globals.GetAppSetting("secretName", log, _config);
            string keyVaultUrl = Globals.GetAppSetting("keyVaultUrl", log, _config);
            string workspaceId = Globals.GetAppSetting("workspaceId", log, _config);
            var storageAccountUrl = Globals.GetAppSetting("storageAccountUrl", log, _config);

            var isLocal = Globals.GetAppSetting("isLocal", log, _config, false);

            //get client secret from keyVault

            var secretClient = new SecretClient(
                new Uri(keyVaultUrl),
                isLocal == "true"
                    ? new AzureCliCredential()
                    : new DefaultAzureCredential()
                );

            KeyVaultSecret secret = secretClient.GetSecret(secretName);

            // Credentials for LogsQueryClient
            var credential = new ClientSecretCredential(
               tenantId,
               clientId,
               secret.Value
            );

            var client = new LogsQueryClient(credential);

            // Create Blob Storage client 

            var blobServiceClient = new BlobServiceClient(
                 new Uri(storageAccountUrl),
                 isLocal == "true"
                     ? new AzureCliCredential()
                     : new DefaultAzureCredential()
             );

            try
            {


                //blob container name and blob name for storing the active users list
                var containerName = "activeusers";
                string blobName = $"{containerName}-{DateTime.UtcNow:dd-MM-yyyy}.json";
                var containerClient = blobServiceClient.GetBlobContainerClient(containerName);
                await containerClient.CreateIfNotExistsAsync(PublicAccessType.None);
                var blobClient = containerClient.GetBlobClient(blobName);


                //get active users from the logs using the LogsQueryClient
               
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
                LogsTable table = response.Value.Table;
                //log.LogInformation($"Response: {table}");

                // Collect users from the Logs table
                var users = new List<ActiveUsersRecord>();

                foreach (var row in table.Rows)
                {
                    var userId = row["UserId"]?.ToString() ?? "";
                    //log.LogInformation($"UserId: {userId}");
                    var email = row["UserPrincipalName"]?.ToString() ?? "";
                    users.Add(new ActiveUsersRecord(userId, email));
                }


                log.LogInformation(users.Count + " active users retrieved.");

                //save data to container
                using var blobStream = await blobClient.OpenWriteAsync(overwrite: true);

                await JsonSerializer.SerializeAsync(
                    blobStream,
                    users,
                    new JsonSerializerOptions
                    {
                        WriteIndented = true
                    }
                );

                await blobStream.FlushAsync();

                log.LogInformation(
                    $"Saved {users.Count} active users to blob: {containerName}/{blobName}"
                );

                return users;
            }
            catch (Exception ex)
            {
                log.LogError(ex.Message);
                throw;
            }
        }

        [Function("ActiveUsers")]

        public async Task Run([TimerTrigger(Globals.TimerStartTime)] TimerInfo myTimer)
        {
            _logger.LogInformation($"Timer trigger function executed at: {DateTime.UtcNow}");

            var activeUsers = await GetActiveUsers(_logger);

            _logger.LogInformation($"Retrieved {activeUsers.Count} active users.");
        }

        //public async Task<IActionResult> Run([HttpTrigger(AuthorizationLevel.Function, "get")] HttpRequest req)
        //{
        

        //    var activeUsers = await GetActiveUsers(_logger);

        //    return new OkObjectResult(activeUsers);
        //}
    }
}

