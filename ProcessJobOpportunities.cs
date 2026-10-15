using Azure.Identity;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Sas;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace GCStats
{
    public class ProcessJobOpportunities
    {
        private readonly ILogger<ProcessJobOpportunities> _logger;
        private readonly IConfiguration _config;

        public ProcessJobOpportunities(ILogger<ProcessJobOpportunities > logger, IConfiguration config)
        {
            _logger = logger;
            _config = config;
        }

        [Function("ProcessJobOpportunities")]
        public async Task Run([QueueTrigger("process-job-opportunities", Connection = "AzureWebJobsStorage")] string blobName)
        {
            _logger.LogInformation("Received blobName: {blobName}", blobName);

            try
            {
                var storageAccountUrl = Auth.GetAppSetting("storageAccountUrl", _logger, _config);
                var isLocal = Auth.GetAppSetting("isLocal", _logger, _config, false);
                var credential = isLocal == "true" ? new AzureCliCredential() : (Azure.Core.TokenCredential)new DefaultAzureCredential();

                var blobServiceClient = new BlobServiceClient(new Uri(storageAccountUrl), credential);

                var date = blobName.Replace($"{JobOpportunities.JobOpportunitiesContainerName}-", string.Empty).Replace(".parquet", string.Empty);
                var skillsBlobName = $"{JobOpportunities.JobOpportunitySkillsContainerName}-{date}.parquet";
                var jobTypesBlobName = $"{JobOpportunities.JobOpportunityJobTypesContainerName}-{date}.parquet";

                var jobOpportunitiesContainerClient = blobServiceClient.GetBlobContainerClient(JobOpportunities.JobOpportunitiesContainerName);
                var skillsContainerClient = blobServiceClient.GetBlobContainerClient(JobOpportunities.JobOpportunitySkillsContainerName);
                var jobTypesContainerClient = blobServiceClient.GetBlobContainerClient(JobOpportunities.JobOpportunityJobTypesContainerName);

                var jobOpportunitiesBlobClient = jobOpportunitiesContainerClient.GetBlobClient(blobName);
                var skillsBlobClient = skillsContainerClient.GetBlobClient(skillsBlobName);
                var jobTypesBlobClient = jobTypesContainerClient.GetBlobClient(jobTypesBlobName);

                if (!await jobOpportunitiesBlobClient.ExistsAsync())
                    throw new FileNotFoundException($"Blob {blobName} not found in container {JobOpportunities.JobOpportunitiesContainerName}");

                if (!await skillsBlobClient.ExistsAsync())
                    throw new FileNotFoundException($"Blob {skillsBlobName} not found in container {JobOpportunities.JobOpportunitySkillsContainerName}");

                if (!await jobTypesBlobClient.ExistsAsync())
                    throw new FileNotFoundException($"Blob {jobTypesBlobName} not found in container {JobOpportunities.JobOpportunityJobTypesContainerName}");

                var delegationKey = await blobServiceClient.GetUserDelegationKeyAsync(
                    new BlobGetUserDelegationKeyOptions(DateTimeOffset.UtcNow.AddMinutes(15))
                    {
                        StartsOn = DateTimeOffset.UtcNow.AddMinutes(-5)
                    }
                );

                using var sqlConnection = await Auth.BuildSqlConnection(_logger, _config);

                await CopyParquetIntoTableAsync(sqlConnection, "dbo.JobOpportunities", jobOpportunitiesContainerClient, jobOpportunitiesBlobClient, delegationKey.Value, blobServiceClient.AccountName);
                _logger.LogInformation("Successfully uploaded job opportunities from {blobName} to dbo.JobOpportunities", blobName);

                await CopyParquetIntoTableAsync(sqlConnection, "dbo.JobOpportunitySkills", skillsContainerClient, skillsBlobClient, delegationKey.Value, blobServiceClient.AccountName);
                _logger.LogInformation("Successfully uploaded skills from {blobName} to dbo.JobOpportunitySkills", skillsBlobName);

                await CopyParquetIntoTableAsync(sqlConnection, "dbo.JobOpportunityJobTypes", jobTypesContainerClient, jobTypesBlobClient, delegationKey.Value, blobServiceClient.AccountName);
                _logger.LogInformation("Successfully uploaded job types from {blobName} to dbo.JobOpportunityJobTypes", jobTypesBlobName);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex.Message);
                throw;
            }

            _logger.LogInformation("Finished processing {blobName}", blobName);
        }

        private static async Task CopyParquetIntoTableAsync(SqlConnection sqlConnection, string destinationTable, BlobContainerClient containerClient, BlobClient blobClient, UserDelegationKey delegationKey, string accountName)
        {
            var sasBuilder = new BlobSasBuilder
            {
                BlobContainerName = containerClient.Name,
                BlobName = blobClient.Name,
                Resource = "b",
                StartsOn = DateTimeOffset.UtcNow.AddMinutes(-5),
                ExpiresOn = DateTimeOffset.UtcNow.AddMinutes(15)
            };
            sasBuilder.SetPermissions(BlobSasPermissions.Read);

            var sasToken = sasBuilder.ToSasQueryParameters(delegationKey, accountName).ToString();

            var blobUrl = blobClient.Uri.ToString().Replace("'", "''");
            var sasSecret = sasToken.Replace("'", "''");

            using var copyCmd = new SqlCommand(
                $"""
                 COPY INTO {destinationTable}
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
        }
    }
}
