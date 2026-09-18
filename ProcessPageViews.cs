using Azure.Identity;
using Azure.Storage.Blobs;
using Azure.Storage.Sas;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace GCStats;

public class ProcessPageViews
{
    private readonly ILogger<ProcessTotalUsers> _logger;
    private readonly IConfiguration _config;

    public ProcessPageViews(ILogger<ProcessTotalUsers> logger, IConfiguration config)
    {
        _logger = logger;
        _config = config;
    }

    [Function("ProcessPageViews")]
    public async Task Run([QueueTrigger("process-page-views", Connection = "")] string blobName)
    {
        _logger.LogInformation("Received blobName: {blobName}", blobName);

        try
        {
            var storageAccountUrl = Auth.GetAppSetting("storageAccountUrl", _logger, _config);
            var isLocal = Auth.GetAppSetting("isLocal", _logger, _config, false);
            var credential = isLocal == "true" ? new AzureCliCredential() : (Azure.Core.TokenCredential)new DefaultAzureCredential();

            var blobServiceClient = new BlobServiceClient(new Uri(storageAccountUrl), credential);
            var containerClient = blobServiceClient.GetBlobContainerClient(PageViews.PageViewsContainerName);
            var blobClient = containerClient.GetBlobClient(blobName);

            if (!await blobClient.ExistsAsync())
                throw new FileNotFoundException($"Blob {blobName} not found in container {PageViews.PageViewsContainerName}");

            var delegationKey = await blobServiceClient.GetUserDelegationKeyAsync(
                new Azure.Storage.Blobs.Models.BlobGetUserDelegationKeyOptions(DateTimeOffset.UtcNow.AddMinutes(15))
                {
                    StartsOn = DateTimeOffset.UtcNow.AddMinutes(-5)
                }
             );

            var sasBuilder = new BlobSasBuilder
            {
                BlobContainerName = containerClient.Name,
                BlobName = blobClient.Name,
                Resource = "b",
                StartsOn = DateTimeOffset.UtcNow.AddMinutes(-5),
                ExpiresOn = DateTimeOffset.UtcNow.AddMinutes(15)
            };
            sasBuilder.SetPermissions(BlobSasPermissions.Read);

            var sasToken = sasBuilder.ToSasQueryParameters(delegationKey.Value, blobServiceClient.AccountName).ToString();

            var blobUrl = blobClient.Uri.ToString().Replace("'", "''");
            var sasSecret = sasToken.Replace("'", "''");

            using var sqlConnection = await Auth.GetSqlConnection(_logger, _config);

            using var copyCmd = new SqlCommand(
                $"""
                     COPY INTO dbo.PageViews
                     FROM '{blobUrl}'
                     WITH (
                         FILE_TYPE = 'PARQUET',
                         CREDENTIAL = (IDENTITY = 'Shared Access Signature', SECRET = '{sasSecret}')
                     );
                     """,
                sqlConnection
            );

            copyCmd.CommandTimeout = 0;
            await copyCmd.ExecuteNonQueryAsync();

            _logger.LogInformation("Successfully uploaded items from {blobName} to dbo.PageViews", blobName);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex.Message);
            throw;
        }

        _logger.LogInformation("Finished processing {blobName}", blobName);
    }
}