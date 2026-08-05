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
    }
}
