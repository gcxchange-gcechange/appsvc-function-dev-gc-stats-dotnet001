using Azure.Core;
using Azure.Identity;
using Azure.Monitor.Query.Logs;
using Azure.Security.KeyVault.Secrets;
using Azure.Storage.Blobs;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Graph;

namespace GCStats
{
    static class Auth
    {
        public static GraphServiceClient GetGraphServiceClient(ILogger log)
        {
            IConfiguration config = new ConfigurationBuilder()
           .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true)
           .AddEnvironmentVariables()
           .Build();

            var scopes = new string[] { "https://graph.microsoft.com/.default" };
            var keyVaultUrl = GetAppSetting("keyVaultUrl", log, config);
            var secretName = GetAppSetting("secretName", log, config);
            var tenantId = GetAppSetting("tenantId", log, config);
            var clientId = GetAppSetting("clientId", log, config);
            var workspaceId = GetAppSetting("workspaceId", log, config);

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
                var isLocal = GetAppSetting("isLocal", log, config, false);
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

        public static async Task<LogsQueryClient> GetLogsQueryClient(ILogger log)
        {
            try
            {
                IConfiguration config = new ConfigurationBuilder()
               .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true)
               .AddEnvironmentVariables()
               .Build();

                var tenantId = GetAppSetting("tenantId", log, config);
                var clientId = GetAppSetting("clientId", log, config);
                var secretName = GetAppSetting("secretName", log, config);
                var keyVaultUrl = GetAppSetting("keyVaultUrl", log, config);
                var isLocal = GetAppSetting("isLocal", log, config, false);

                var secretClient = new SecretClient(new Uri(keyVaultUrl), isLocal == "true" ? new AzureCliCredential() : new DefaultAzureCredential());

                KeyVaultSecret secret = secretClient.GetSecret(secretName);

                var credential = new ClientSecretCredential(tenantId, clientId, secret.Value);
                var logsQueryClient = new LogsQueryClient(credential);

                return logsQueryClient;
            }
            catch (Exception ex)
            {
                log.LogError(ex.ToString());
                throw;
            }
        }

        public static async Task<SqlConnection> GetSqlConnection(ILogger log, IConfiguration config)
        {
            try
            {
                var warehouseServer = GetAppSetting("fabricWarehouseServer", log, config);
                var warehouseDatabase = GetAppSetting("fabricWarehouseDatabase", log, config);
                var isLocal = GetAppSetting("isLocal", log, config, false);

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
                log.LogError(ex.ToString());
                throw;
            }
        }

        public static async Task<BlobClient> GetBlobClient(string containerName, string blobName, ILogger log, IConfiguration config)
        {
            try
            {
                var storageAccountUrl = GetAppSetting("storageAccountUrl", log, config);
                var isLocal = GetAppSetting("isLocal", log, config, false);

                var blobServiceClient = new BlobServiceClient(new Uri(storageAccountUrl), isLocal == "true" ? new AzureCliCredential() : new DefaultAzureCredential());
                var containerClient = blobServiceClient.GetBlobContainerClient(containerName);

                await containerClient.CreateIfNotExistsAsync(Azure.Storage.Blobs.Models.PublicAccessType.None);

                return containerClient.GetBlobClient(blobName);

            }
            catch (Exception ex)
            {
                log.LogError(ex.ToString());
                throw;
            }
        }

        public static string GetAppSetting(string settingName, ILogger log, IConfiguration config, bool isMandatory = true)
        {
            var value = config[settingName];

            if (value == null && isMandatory)
            {
                var msg = $"{settingName} is missing from the environment variables or local.settings.json";
                log.LogError(msg);
                throw new MissingFieldException(msg);
            }

            return value ?? string.Empty;
        }
    }
}
