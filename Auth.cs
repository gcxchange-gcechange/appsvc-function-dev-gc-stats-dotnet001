using Azure.Core;
using Azure.Identity;
using Azure.Security.KeyVault.Secrets;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Graph;

namespace GCStats
{
    static class Auth
    {
        public static GraphServiceClient GraphAuth(ILogger log)
        {
            IConfiguration config = new ConfigurationBuilder()
           .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true)
           .AddEnvironmentVariables()
           .Build();

            var scopes = new string[] { "https://graph.microsoft.com/.default" };
            var keyVaultUrl = Globals.GetAppSetting("keyVaultUrl", log, config);
            var secretName = Globals.GetAppSetting("secretName", log, config);
            var tenantId = Globals.GetAppSetting("tenantId", log, config);
            var clientId = Globals.GetAppSetting("clientId", log, config);

            SecretClientOptions options = new SecretClientOptions()
            {
                Retry =
                {
                    Delay= TimeSpan.FromSeconds(2),
                    MaxDelay = TimeSpan.FromSeconds(16),
                    MaxRetries = 5,
                    Mode = RetryMode.Exponential
                 }
            };

            SecretClient client;
            KeyVaultSecret secret;

            try
            {
                var isLocal = Globals.GetAppSetting("isLocal", log, config, false);
                client = new SecretClient(new Uri(keyVaultUrl), isLocal == "true" ? new AzureCliCredential() : new DefaultAzureCredential(), options);
                secret = client.GetSecret(secretName);
            }
            catch (Exception ex)
            {
                log.LogError(ex.ToString());
                throw;
            }

            var optionsToken = new TokenCredentialOptions
            {
                AuthorityHost = AzureAuthorityHosts.AzurePublicCloud
            };

            var clientSecretCredential = new ClientSecretCredential(tenantId, clientId, secret.Value, optionsToken);

            var graphClient = new GraphServiceClient(clientSecretCredential, scopes);
            return graphClient;
        }

        public static async Task<SqlConnection> GetSqlConnection(ILogger log, IConfiguration config)
        {
            try
            {
                var warehouseServer = Globals.GetAppSetting("fabricWarehouseServer", log, config);
                var warehouseDatabase = Globals.GetAppSetting("fabricWarehouseDatabase", log, config);
                var isLocal = Globals.GetAppSetting("isLocal", log, config, false);

                // requires TCP 1433
                var connectionString =
                $"Server=tcp:{warehouseServer},1433;" +
                $"Initial Catalog={warehouseDatabase};" +
                "TrustServerCertificate=False;" +
                "Encrypt=True;";

                var connection = new SqlConnection(connectionString);

                var tokenCredential = isLocal == "true" ? (TokenCredential)new AzureCliCredential() : new ManagedIdentityCredential();

                var tokenContext = new TokenRequestContext(new[] { "https://database.windows.net/.default" });
                var accessToken = await tokenCredential.GetTokenAsync(tokenContext, CancellationToken.None);

                connection.AccessToken = accessToken.Token;

                await connection.OpenAsync();

                log.LogInformation("Connected to SQL Server: {server}, Database: {database}", warehouseServer, warehouseDatabase);

                return connection;
            } 
            catch (Exception ex)
            {
                log.LogError(ex.Message);
                throw;
            }
        }
    }
}
