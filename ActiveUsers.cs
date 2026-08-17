using Azure;
using Azure.Identity;
using Azure.Monitor.Query.Logs;
using Azure.Monitor.Query.Logs.Models;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Azure.Security.KeyVault.Secrets;


namespace GCStats
{

    public record ActiveUsersRecord(
     string Id,
     string DisplayName,
     string Email,
     string UserPrincipalName
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

            try
            {
                var isLocal = Globals.GetAppSetting("isLocal", log, _config, false);
                var graph = new Auth().GraphAuth(log);

                var secretClient = new SecretClient(
                    new Uri(keyVaultUrl),
                    isLocal == "true"
                        ? new AzureCliCredential()
                        : new DefaultAzureCredential()
                );

                KeyVaultSecret secret = secretClient.GetSecret(secretName);

                var credential = new ClientSecretCredential(
                    tenantId,
                    clientId,
                    secret.Value
                );

                var client = new LogsQueryClient(credential);


                Response<LogsQueryResult> response = await client.QueryWorkspaceAsync(

                    workspaceId: workspaceId,
                    query: "SigninLogs | where ResultType == 0 | summarize ActiveUsers = dcount(UserPrincipalName) by bin(TimeGenerated, 1d)",
                    timeRange: new LogsQueryTimeRange(TimeSpan.FromDays(7))
                );

                foreach (var table in response.Value.AllTables)
                {
                    foreach (var row in table.Rows)
                    {
                        foreach (var value in row)
                        {
                            Console.Write($"{value} | ");
                        }
                         
                        Console.WriteLine();
                    }
                }



                // Get users 
                var activeUsers = await graph.Users.GetAsync(rc =>
                    {
                        rc.Headers.Add("ConsistencyLevel", "eventual");
                        rc.QueryParameters.Top = 100;
                        //rc.QueryParameters.Select = new[] { "id", "displayName", "mail", "userPrincipalName" };
                    }
                );
             

                var users = new List<ActiveUsersRecord>();

                log.LogInformation(users.Count + " active users retrieved.");

                if (activeUsers?.Value != null)
                {
                    foreach (var user in activeUsers.Value)
                    {
                        users.Add(new ActiveUsersRecord(
                            user.Id ?? "",
                            user.DisplayName ?? "",
                            user.Mail ?? "",
                            user.UserPrincipalName ?? ""
                        ));
                    }
                }

                
                return users;
            }
            catch (Exception ex)
            {
                log.LogError(ex.Message);
                throw;
            }
        }

        [Function("ActiveUsers")]
        public async Task<IActionResult> Run([HttpTrigger(AuthorizationLevel.Function, "get")] HttpRequest req)
        {
        

            var activeUsers = await GetActiveUsers(_logger);

            return new OkObjectResult(activeUsers);
        }
    }
}

