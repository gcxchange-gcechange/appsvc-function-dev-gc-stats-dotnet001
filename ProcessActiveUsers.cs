using Azure.Identity;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Sas;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System.Data;
using System.Text.Json;

namespace GCStats
{
    public class ProcessActiveUsers
    {
        private readonly ILogger<ProcessActiveUsers> _logger;
        private readonly IConfiguration _config;

        public ProcessActiveUsers(ILogger<ProcessActiveUsers> logger, IConfiguration config)
        {
            _logger = logger;
            _config = config;
        }

        [Function("ProcessActiveUsers")]
        public async Task Run([QueueTrigger("process-active-users", Connection = "AzureWebJobsStorage")] string blobName)
        {
            _logger.LogInformation("Received blobName: {blobName}", blobName);

            try
            {
                var storageAccountUrl = Auth.GetAppSetting("storageAccountUrl", _logger, _config);
                var isLocal = Auth.GetAppSetting("isLocal", _logger, _config, false);
                var credential = isLocal == "true" ? new AzureCliCredential() : (Azure.Core.TokenCredential)new DefaultAzureCredential();

                var blobServiceClient = new BlobServiceClient(new Uri(storageAccountUrl), credential);
                var containerClient = blobServiceClient.GetBlobContainerClient(Users.ActiveUsersContainerName);
                var blobClient = containerClient.GetBlobClient(blobName);

                if (!await blobClient.ExistsAsync())
                    throw new FileNotFoundException($"Blob {blobName} not found in container {Users.ActiveUsersContainerName}");

                var now = DateTimeOffset.UtcNow;

                var sasStartsOn = now.AddMinutes(-5);
                var sasExpiresOn = now.AddMinutes(15);
                var delegationKeyExpiresOn = sasExpiresOn.AddMinutes(5);

                var delegationKey = await blobServiceClient.GetUserDelegationKeyAsync(
                    new Azure.Storage.Blobs.Models.BlobGetUserDelegationKeyOptions(
                        delegationKeyExpiresOn)
                    {
                        StartsOn = sasStartsOn
                    });

                var sasBuilder = new BlobSasBuilder
                {
                    BlobContainerName = containerClient.Name,

                    // Do NOT set BlobName for a container SAS.

                    Resource = "c",
                    Protocol = SasProtocol.Https,
                    StartsOn = sasStartsOn,
                    ExpiresOn = sasExpiresOn
                };

                sasBuilder.SetPermissions(
    BlobContainerSasPermissions.Read |
    BlobContainerSasPermissions.List);

                // Safe diagnostics: do not log the SAS token, signature, or SAS URI.
                _logger.LogInformation(
                    "Generated SAS metadata for {BlobName}: Resource={Resource}; Permissions={Permissions}; StartsOn={StartsOn:O}; ExpiresOn={ExpiresOn:O}",
                    blobClient.Name,
                    sasBuilder.Resource,
                    sasBuilder.Permissions,
                    sasBuilder.StartsOn,
                    sasBuilder.ExpiresOn);

                var sasToken = sasBuilder
                    .ToSasQueryParameters(
                        delegationKey.Value,
                        blobServiceClient.AccountName)
                    .ToString();

                var sasUri = new Uri($"{blobClient.Uri}?{sasToken}");


                try
                {

                    var sasBlobClient = new BlobClient(sasUri);
                    var properties = await sasBlobClient.GetPropertiesAsync();

                    _logger.LogInformation(
                        "SAS validation succeeded for {BlobName}. Length={Length}; ETag={ETag}",
                        blobClient.Name,
                        properties.Value.ContentLength,
                        properties.Value.ETag);
                }
                catch (Azure.RequestFailedException ex)
                {
                    _logger.LogError(
                        ex,
                        "SAS validation failed for {BlobName}. HTTP status={Status}; Storage error={ErrorCode}",
                        blobClient.Name,
                        ex.Status,
                        ex.ErrorCode);

                    throw;
                }

                var sasContainerUri = new Uri($"{containerClient.Uri}?{sasToken}");
                var sasContainerClient = new BlobContainerClient(sasContainerUri);

                var foundBlob = false;

                await foreach (var item in sasContainerClient.GetBlobsAsync( traits: BlobTraits.None, states: BlobStates.None, prefix: blobClient.Name, cancellationToken: CancellationToken.None))
                {
                    if (item.Name == blobClient.Name)
                    {
                        foundBlob = true;
                        break;
                    }
                }

                if (!foundBlob)
                {
                    throw new FileNotFoundException(
                        $"SAS could list the container but did not find {blobClient.Name}");
                }

                _logger.LogInformation(
                    "SAS read/list validation succeeded for {BlobName}",
                    blobClient.Name);


                

                using var sqlConnection = await Auth.BuildSqlConnection(_logger, _config);

                var blobUrl = blobClient.Uri.ToString().Replace("'", "''");

                using var copyCmd = new SqlCommand(
                    $"""
                         COPY INTO dbo.ActiveUsers
                         FROM '{blobUrl}'
                         WITH (
                             FILE_TYPE = 'PARQUET',
                             CREDENTIAL = (IDENTITY = 'Workspace Identity')
                         );
                         """,
                    sqlConnection
                );

                copyCmd.CommandTimeout = 0;
                await copyCmd.ExecuteNonQueryAsync();

                _logger.LogInformation("Successfully uploaded users from {blobName} to dbo.ActiveUsers", blobName);
            }
            catch (Exception ex)
            {
                _logger.LogError(
                    ex,
                    "Failed to process active-users blob {BlobName}",
                    blobName);

                throw;
            }
        }
    }
}
