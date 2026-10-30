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

        public static async Task<string> StreamUsersToBlobAsync(ILogger log, IConfiguration config, CancellationToken ct = default)
        {
            try
            {
                var snapshotDate = DateTime.UtcNow.Date;
                var blobName = $"{TotalUsersContainerName}-{DateTime.UtcNow.ToString(Globals.BlobDateFormat)}.parquet";
                var blobClient = await Auth.GetBlobClient(TotalUsersContainerName, blobName, log, config);

                using var blobStream = await blobClient.OpenWriteAsync(overwrite: true, cancellationToken: ct);

                var idField = new DataField<string>("Id");
                var mailField = new DataField<string?>("Mail");
                var domainField = new DataField<string?>("Domain");
                var upnField = new DataField<string?>("UserPrincipalName");
                var createdDateTimeField = new DataField<DateTime?>("CreatedDateTime");
                var snapshotDateField = new DataField<DateTime>("SnapshotDate");

                var schema = new ParquetSchema(idField, mailField, domainField, upnField, createdDateTimeField, snapshotDateField);

                await using var parquetWriter = await ParquetWriter.CreateAsync(schema, blobStream, Globals.ParquetOptions, cancellationToken: ct);

                var graph = Auth.GetGraphServiceClient(log);
                int count = 0;

                var idBuffer = new List<string>(Globals.RowGroupBatchSize);
                var mailBuffer = new List<string?>(Globals.RowGroupBatchSize);
                var domainBuffer = new List<string?>(Globals.RowGroupBatchSize);
                var upnBuffer = new List<string?>(Globals.RowGroupBatchSize);
                var createdDateTimeBuffer = new List<DateTime?>(Globals.RowGroupBatchSize);
                var snapshotDateBuffer = new List<DateTime>(Globals.RowGroupBatchSize);

                async Task FlushBatchAsync()
                {
                    ct.ThrowIfCancellationRequested();

                    if (idBuffer.Count == 0)
                        return;

                    using var groupWriter = parquetWriter.CreateRowGroup();

                    await groupWriter.WriteAsync(idField, idBuffer);
                    await groupWriter.WriteAsync(mailField, mailBuffer);
                    await groupWriter.WriteAsync(domainField, domainBuffer);
                    await groupWriter.WriteAsync(upnField, upnBuffer);
                    await groupWriter.WriteAsync<DateTime>(createdDateTimeField, createdDateTimeBuffer.ToArray().AsMemory(), cancellationToken: ct);
                    await groupWriter.WriteAsync<DateTime>(snapshotDateField, snapshotDateBuffer.ToArray().AsMemory(), cancellationToken: ct);

                    idBuffer.Clear();
                    mailBuffer.Clear();
                    domainBuffer.Clear();
                    upnBuffer.Clear();
                    createdDateTimeBuffer.Clear();
                    snapshotDateBuffer.Clear();
                }

                var usersPage = await graph.Users.GetAsync((requestConfiguration) =>
                {
                    requestConfiguration.Headers.Add("ConsistencyLevel", "eventual");
                    requestConfiguration.QueryParameters.Top = 999;
                    requestConfiguration.QueryParameters.Select = [
                        "Id",
                        "Mail",
                        "UserPrincipalName",
                        "CreatedDateTime",
                    ];
                }, ct);

                if (usersPage is null)
                    throw new InvalidOperationException("Graph returned no users page; refusing to write an empty snapshot.");

                var exceptionUsersArray = Auth.GetAppSetting("exceptionUsersArray", log, config);

                var pageIterator = PageIterator<User, UserCollectionResponse>
                    .CreatePageIterator(
                        graph,
                        usersPage,
                        async user =>
                        {
                            ct.ThrowIfCancellationRequested();

                            if (user.Id != null && !exceptionUsersArray.Contains(user.Id))
                            {
                                idBuffer.Add(user.Id);
                                mailBuffer.Add(user.Mail);
                                domainBuffer.Add(GetUserDomain(user));
                                upnBuffer.Add(user.UserPrincipalName);
                                createdDateTimeBuffer.Add(user.CreatedDateTime?.UtcDateTime);
                                snapshotDateBuffer.Add(snapshotDate);
                                count++;

                                if (idBuffer.Count >= Globals.RowGroupBatchSize)
                                    await FlushBatchAsync();
                            }

                            return true;
                        },
                        requestConfiguration =>
                        {
                            requestConfiguration.Headers.Add("ConsistencyLevel", "eventual");
                            return requestConfiguration;
                        });

                await pageIterator.IterateAsync(ct);
                await FlushBatchAsync();
                await parquetWriter.DisposeAsync();

                log.LogInformation("Streamed {Count} users to blob {BlobName}", count, blobName);

                return blobName;
            }
            catch (OperationCanceledException)
            {
                log.LogWarning("StreamUsersToBlobAsync was cancelled");
                throw;
            }
            catch (Exception ex)
            {
                log.LogError(ex, "StreamUsersToBlobAsync failed");
                throw;
            }
        }

        private static string? GetUserDomain(User user)
        {
            var email = user.Mail ?? user.OtherMails?.FirstOrDefault();
            if (!string.IsNullOrWhiteSpace(email) && email.Contains('@'))
                return email.Split('@').Last().ToLowerInvariant();

            var upn = user.UserPrincipalName;
            if (string.IsNullOrWhiteSpace(upn))
                return null;

            var guestDomain = GetGuestUserDomain(upn);
            if (guestDomain != null)
                return guestDomain;

            var at = upn.LastIndexOf('@');
            if (at >= 0 && at < upn.Length - 1)
                return upn.Substring(at + 1).ToLowerInvariant();

            return null;
        }

        private static string? GetGuestUserDomain(string? upn)
        {
            if (string.IsNullOrWhiteSpace(upn))
                return null;

            var extIndex = upn.IndexOf("#EXT#", StringComparison.OrdinalIgnoreCase);
            if (extIndex < 0)
                return null;

            var beforeExt = upn.Substring(0, extIndex);
            var underscore = beforeExt.LastIndexOf('_');
            if (underscore < 0 || underscore == beforeExt.Length - 1)
                return null;

            return beforeExt.Substring(underscore + 1).ToLowerInvariant();
        }
    }
}