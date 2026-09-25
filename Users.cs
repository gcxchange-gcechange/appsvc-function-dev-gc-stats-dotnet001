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
                var mailField = new DataField<string?>("Mail");
                var upnField = new DataField<string?>("UserPrincipalName");
                var displayNameField = new DataField<string?>("DisplayName");
                var userTypeField = new DataField<string?>("UserType");
                var accountEnabledField = new DataField<bool?>("AccountEnabled");
                var preferredLanguageField = new DataField<string?>("PreferredLanguage");
                var createdDateTimeField = new DataField<DateTime?>("CreatedDateTime");
                var lastPasswordChangeField = new DataField<DateTime?>("LastPasswordChangeDateTime");
                var externalUserStateField = new DataField<string?>("ExternalUserState");
                var snapshotDateField = new DataField<DateTime>("SnapshotDate");

                var schema = new ParquetSchema(idField, mailField, upnField, displayNameField, userTypeField, accountEnabledField, preferredLanguageField,
                    createdDateTimeField, lastPasswordChangeField, externalUserStateField, snapshotDateField);

                await using var parquetWriter = await ParquetWriter.CreateAsync(schema, blobStream, Globals.ParquetOptions);

                var graph = Auth.GetGraphServiceClient(log);
                int count = 0;

                var idBuffer = new List<string>(Globals.RowGroupBatchSize);
                var mailBuffer = new List<string?>(Globals.RowGroupBatchSize);
                var upnBuffer = new List<string?>(Globals.RowGroupBatchSize);
                var displayNameBuffer = new List<string?>(Globals.RowGroupBatchSize);
                var userTypeBuffer = new List<string?>(Globals.RowGroupBatchSize);
                var accountEnabledBuffer = new List<bool?>(Globals.RowGroupBatchSize);
                var preferredLanguageBuffer = new List<string?>(Globals.RowGroupBatchSize);
                var createdDateTimeBuffer = new List<DateTime?>(Globals.RowGroupBatchSize);
                var lastPasswordChangeBuffer = new List<DateTime?>(Globals.RowGroupBatchSize);
                var externalUserStateBuffer = new List<string?>(Globals.RowGroupBatchSize);
                var snapshotDateBuffer = new List<DateTime>(Globals.RowGroupBatchSize);

                async Task FlushBatchAsync()
                {
                    if (idBuffer.Count == 0) 
                        return;

                    using var groupWriter = parquetWriter.CreateRowGroup();

                    await groupWriter.WriteAsync(idField, idBuffer);
                    await groupWriter.WriteAsync(mailField, mailBuffer);
                    await groupWriter.WriteAsync(upnField, upnBuffer);
                    await groupWriter.WriteAsync(displayNameField, displayNameBuffer);
                    await groupWriter.WriteAsync(userTypeField, userTypeBuffer);
                    await groupWriter.WriteAsync<bool>(accountEnabledField, accountEnabledBuffer.ToArray().AsMemory());
                    await groupWriter.WriteAsync(preferredLanguageField, preferredLanguageBuffer);
                    await groupWriter.WriteAsync<DateTime>(createdDateTimeField, createdDateTimeBuffer.ToArray().AsMemory());
                    await groupWriter.WriteAsync<DateTime>(lastPasswordChangeField, lastPasswordChangeBuffer.ToArray().AsMemory());
                    await groupWriter.WriteAsync(externalUserStateField, externalUserStateBuffer);
                    await groupWriter.WriteAsync<DateTime>(snapshotDateField, snapshotDateBuffer.ToArray().AsMemory());

                    idBuffer.Clear();
                    mailBuffer.Clear();
                    upnBuffer.Clear();
                    displayNameBuffer.Clear();
                    userTypeBuffer.Clear();
                    accountEnabledBuffer.Clear();
                    preferredLanguageBuffer.Clear();
                    createdDateTimeBuffer.Clear();
                    lastPasswordChangeBuffer.Clear();
                    externalUserStateBuffer.Clear();
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
                        "DisplayName",
                        "AccountEnabled", 
                        "CreatedDateTime", 
                        "UserType",
                        "PreferredLanguage",
                        "LastPasswordChangeDateTime",
                        "ExternalUserState"
                    ];
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
                                mailBuffer.Add(user.Mail);
                                upnBuffer.Add(user.UserPrincipalName);
                                displayNameBuffer.Add(user.DisplayName);
                                userTypeBuffer.Add(user.UserType);
                                accountEnabledBuffer.Add(user.AccountEnabled);
                                preferredLanguageBuffer.Add(user.PreferredLanguage);
                                createdDateTimeBuffer.Add(user.CreatedDateTime?.UtcDateTime);
                                lastPasswordChangeBuffer.Add(user.LastPasswordChangeDateTime?.UtcDateTime);
                                externalUserStateBuffer.Add(user.ExternalUserState);
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
