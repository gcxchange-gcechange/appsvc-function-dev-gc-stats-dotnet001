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

        public static async Task<string> StreamUsersToBlobAsync(ILogger log, IConfiguration config)
        {
            try
            {
                var snapshotDate = DateTime.UtcNow.Date;
                var blobName = $"{TotalUsersContainerName}-{DateTime.UtcNow.ToString(Globals.BlobDateFormat)}.parquet";
                var blobClient = await Auth.GetBlobClient(TotalUsersContainerName, blobName, log, config);

                using var blobStream = await blobClient.OpenWriteAsync(overwrite: true);

                var idField = new DataField<string>("Id");
                var mailField = new DataField<string>("Mail");
                var snapshotDateField = new DataField<DateTime>("SnapshotDate");
                var schema = new ParquetSchema(idField, mailField, snapshotDateField);

                await using var parquetWriter = await ParquetWriter.CreateAsync(schema, blobStream, Globals.ParquetOptions);

                var graph = Auth.GetGraphServiceClient(log);
                int count = 0;

                var idBuffer = new List<string>(Globals.RowGroupBatchSize);
                var mailBuffer = new List<string>(Globals.RowGroupBatchSize);
                var snapshotDateBuffer = new List<DateTime>(Globals.RowGroupBatchSize);

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

                var exceptionUsersArray = Auth.GetAppSetting("exceptionUsersArray", log, config);

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

                                if (idBuffer.Count >= Globals.RowGroupBatchSize)
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
