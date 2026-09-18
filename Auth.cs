using Azure.Core;
using Azure.Identity;
using Azure.Monitor.Query.Logs;
using Azure.Security.KeyVault.Secrets;
using Azure.Storage.Blobs;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Graph;
using Microsoft.Kiota.Http.HttpClientLibrary.Middleware.Options;

namespace GCStats
{
    static class Auth
    {
        private static GraphServiceClient? _graphClient;
        private static DateTimeOffset _graphCreatedAt;
        private static readonly TimeSpan _graphMaxAge = TimeSpan.FromHours(23);
        private static readonly object _graphLock = new object();


        public static GraphServiceClient GetGraphServiceClient(ILogger log)
        {
            if (_graphClient != null)
                return _graphClient;

            lock (_graphLock)
            {
                if (_graphClient == null || DateTimeOffset.UtcNow - _graphCreatedAt >= _graphMaxAge)
                {
                    _graphClient = BuildGraphServiceClient(log);
                    _graphCreatedAt = DateTimeOffset.UtcNow;
                }
            }

            return _graphClient;
        }

        public static GraphServiceClient BuildGraphServiceClient(ILogger log)
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
                    Delay = TimeSpan.FromSeconds(2),
                    MaxDelay = TimeSpan.FromSeconds(60),
                    MaxRetries = 8,
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
            var maxRetries = 10;

            var retryOption = new RetryHandlerOption
            {
                MaxRetry = maxRetries,
                ShouldRetry = (delay, attempt, response) =>
                {
                    if (response == null)
                        return false;

                    var shouldRetry = response.StatusCode == System.Net.HttpStatusCode.TooManyRequests ||
                                    response.StatusCode == System.Net.HttpStatusCode.ServiceUnavailable ||
                                    response.StatusCode == System.Net.HttpStatusCode.GatewayTimeout;

                    if (shouldRetry)
                        log.LogWarning($"Graph ({response.StatusCode}) - Retrying request (Attempt {attempt + 1} of {maxRetries}). Waiting {delay} seconds...");

                    return shouldRetry;
                }
            };

            var handlers = GraphClientFactory.CreateDefaultHandlers().ToList();

            handlers.RemoveAll(h => h is Microsoft.Kiota.Http.HttpClientLibrary.Middleware.RetryHandler);
            handlers.Add(new Microsoft.Kiota.Http.HttpClientLibrary.Middleware.RetryHandler(retryOption));

            var httpClient = GraphClientFactory.Create(handlers);

            var graphClient = new GraphServiceClient(httpClient, clientSecretCredential, scopes);

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
