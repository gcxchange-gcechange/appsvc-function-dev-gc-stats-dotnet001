using Azure.Identity;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Graph;
using Microsoft.Graph.Models;
using Parquet;
using Parquet.Schema;

namespace GCStats
{
    public record UserRecord(string Id, string Mail);

    static class Users
    {
        public static readonly string[] UserQuerySelectParams = ["id", "mail"];

        public const string TotalUsersContainerName = "users";
        public const string ActiveUsersContainerName = "active-users";

        private const int RowGroupBatchSize = 50_000;

        public static async Task<string> StreamUsersToBlobAsync(ILogger log, IConfiguration config)
        {
            try
            {
                var storageAccountUrl = Globals.GetAppSetting("storageAccountUrl", log, config);
                var exceptionUsersArray = Globals.GetAppSetting("exceptionUsersArray", log, config);
                var isLocal = Globals.GetAppSetting("isLocal", log, config, false);

                var snapshotDate = DateTime.UtcNow.Date;
                var blobName = $"users-{DateTime.UtcNow:yyyy-MM-dd}.parquet";

                var blobServiceClient = new BlobServiceClient(new Uri(storageAccountUrl), isLocal == "true" ? new AzureCliCredential() : new DefaultAzureCredential());
                var containerClient = blobServiceClient.GetBlobContainerClient(TotalUsersContainerName);
                await containerClient.CreateIfNotExistsAsync(PublicAccessType.None);
                var blobClient = containerClient.GetBlobClient(blobName);

                var idField = new DataField<string>("Id");
                var mailField = new DataField<string>("Mail");
                var snapshotDateField = new DataField<DateTime>("SnapshotDate");
                var schema = new ParquetSchema(idField, mailField, snapshotDateField);

                var parquetOptions = new ParquetOptions
                {
                    CompressionMethod = CompressionMethod.Snappy
                };

                using var blobStream = await blobClient.OpenWriteAsync(overwrite: true);

                await using var parquetWriter = await ParquetWriter.CreateAsync(schema, blobStream, parquetOptions);

                var graph = Auth.GraphAuth(log);
                int count = 0;

                var idBuffer = new List<string>(RowGroupBatchSize);
                var mailBuffer = new List<string>(RowGroupBatchSize);
                var snapshotDateBuffer = new List<DateTime>(RowGroupBatchSize);

                async Task FlushBatchAsync()
                {
                    if (idBuffer.Count == 0) 
                        return;

                    using var groupWriter = parquetWriter.CreateRowGroup();

                    await groupWriter.WriteAsync(idField, idBuffer);
                    await groupWriter.WriteAsync(mailField, mailBuffer);
                    await groupWriter.WriteAsync<DateTime>(snapshotDateField, snapshotDateBuffer.ToArray().AsMemory());

                    idBuffer.Clear();
                    mailBuffer.Clear();
                    snapshotDateBuffer.Clear();
                }

                var usersPage = await graph.Users.GetAsync((requestConfiguration) =>
                {
                    requestConfiguration.Headers.Add("ConsistencyLevel", "eventual");
                    requestConfiguration.QueryParameters.Top = 999;
                    requestConfiguration.QueryParameters.Select = Users.UserQuerySelectParams;
                });

                var pageIterator = PageIterator<User, UserCollectionResponse>
                    .CreatePageIterator(
                        graph,
                        usersPage!,
                        user =>
                        {
                            if (user.Id != null && !exceptionUsersArray.Contains(user.Id))
                            {
                                idBuffer.Add(user.Id);
                                mailBuffer.Add(user.Mail ?? string.Empty);
                                snapshotDateBuffer.Add(snapshotDate);
                                count++;

                                if (idBuffer.Count >= RowGroupBatchSize)
                                {
                                    FlushBatchAsync().GetAwaiter().GetResult();
                                }
                            }

                            return true;
                        },
                        requestConfiguration =>
                        {
                            requestConfiguration.Headers.Add("ConsistencyLevel", "eventual");
                            return requestConfiguration;
                        });

                await pageIterator.IterateAsync();
                await FlushBatchAsync();
                await parquetWriter.DisposeAsync();

                log.LogInformation("Streamed {Count} users to blob {BlobName}", count, blobName);

                return blobName;
            }
            catch (Exception ex)
            {
                log.LogError("StreamUsersToBlobAsync failed");
                log.LogError(ex.Message);
                throw;
            }
        }
    }
}
