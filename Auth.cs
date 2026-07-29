using Azure.Core;
using Azure.Identity;
using Azure.Security.KeyVault.Secrets;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Graph;

namespace GCStats
{
    class Auth
    {
        public GraphServiceClient GraphAuth(ILogger log)
        {
            IConfiguration config = new ConfigurationBuilder()
           .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true)
           .AddEnvironmentVariables()
           .Build();

            var scopes = new string[] { "https://graph.microsoft.com/.default" };
            var keyVaultUrl = config["keyVaultUrl"];
            var secretName = config["secretName"];
            var tenantId = config["tenantId"];
            var clientId = config["clientId"];

            if (string.IsNullOrWhiteSpace(tenantId))
                throw new InvalidOperationException("Missing \"tenantId\" for Graph authentication.");

            if (string.IsNullOrWhiteSpace(clientId))
                throw new InvalidOperationException("Missing \"clientId\" for Graph authentication.");

            if (string.IsNullOrWhiteSpace(keyVaultUrl))
                throw new InvalidOperationException("Missing \"keyVaultUrl\" for Graph authentication.");

            if (string.IsNullOrWhiteSpace(secretName))
                throw new InvalidOperationException("Missing \"secretName\" for Graph authentication.");

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
                var isLocal = config["isLocal"];
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
    }
}
