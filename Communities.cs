using Azure.Identity;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using CsvHelper;
using GCStats.Models;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Graph;
using Microsoft.Graph.Models;
using Parquet;
using Parquet.Schema;
using System.Globalization;

namespace GCStats
{
    public record CommunityRecord(
        string Id, 
        string DisplayName, 
        UserRecord[] OwnerList,
        UserRecord[] MemberList,
        SensitivityLabelRecord SensitivityLabel,
        DateTimeOffset CreationDate,
        DateTimeOffset LastActivityDate
    );

    public record SensitivityLabelRecord(string Id, string DisplayName);

    static class Communities
    {
        public const string TotalCommunitiesContainerName = "communities";
        public const string CommunityOwnersContainerName = "community-owners";
        public const string CommunityMembersContainerName = "community-members";
        private const int RowGroupBatchSize = 50_000;

        public static async Task<string> StreamCommunitiesToBlobAsync(ILogger log, IConfiguration config)
        {
            try
            {
                var graph = Auth.GraphAuth(log);

                // Get teams activity report
                using var teamsUsageStream = await graph.Reports.GetTeamsTeamActivityDetailWithPeriod("D7").GetAsync();
                using var teamsUsageReader = new StreamReader(teamsUsageStream);
                using var teamsUsage = new CsvReader(teamsUsageReader, CultureInfo.InvariantCulture);
                var teamsActivityRecords = teamsUsage.GetRecords<TeamActivityRecord>();

                // Get sharepoint usage report
                using var sharepointUsageStream = await graph.Reports.GetSharePointSiteUsageDetailWithPeriod("D7").GetAsync();
                using var sharepointUsageReader = new StreamReader(sharepointUsageStream);
                using var sharepointUsage = new CsvReader(sharepointUsageReader, CultureInfo.InvariantCulture);
                var sharepointUsageRecords = sharepointUsage.GetRecords<SharePointUsageRecord>();

                var storageAccountUrl = Globals.GetAppSetting("storageAccountUrl", log, config);
                var exceptionGroupsArray = Globals.GetAppSetting("exceptionGroupsArray", log, config);
                var isLocal = Globals.GetAppSetting("isLocal", log, config, false);

                var snapshotDate = DateTime.UtcNow.Date;
                var communitiesBlobName = $"communities-{DateTime.UtcNow.ToString(Globals.BlobDateFormat)}.parquet";
                var ownersBlobName = $"community-owners-{DateTime.UtcNow.ToString(Globals.BlobDateFormat)}.parquet";
                var membersBlobName = $"community-members-{DateTime.UtcNow.ToString(Globals.BlobDateFormat)}.parquet";

                // TotalCommunities Fields
                var idField = new DataField<string>("Id");
                var displayNameField = new DataField<string>("DisplayName");
                var sensitivityLabelIdField = new DataField<string>("SensitivityLabelId");
                var creationDateField = new DataField<DateTime>("CreationDate");
                var lastActivityDateField = new DataField<DateTime>("LastActivityDate");

                // Community Owner/Member Fields
                var communityIdField = new DataField<string>("CommunityId");
                var userIdField = new DataField<string>("UserId");

                // Shared Fields
                var snapshotDateField = new DataField<DateTime>("SnapshotDate");

                var communitySchema = new ParquetSchema(idField, displayNameField, sensitivityLabelIdField, creationDateField, lastActivityDateField, snapshotDateField);
                var ownerSchema = new ParquetSchema(communityIdField, userIdField, snapshotDateField);
                var memberSchema = new ParquetSchema(communityIdField, userIdField, snapshotDateField);

                var blobServiceClient = new BlobServiceClient(new Uri(storageAccountUrl), isLocal == "true" ? new AzureCliCredential() : new DefaultAzureCredential());

                var communitiesContainerClient = blobServiceClient.GetBlobContainerClient(TotalCommunitiesContainerName);
                await communitiesContainerClient.CreateIfNotExistsAsync(PublicAccessType.None);
                var communitiesBlobClient = communitiesContainerClient.GetBlobClient(communitiesBlobName);

                var ownersContainerClient = blobServiceClient.GetBlobContainerClient(CommunityOwnersContainerName);
                await ownersContainerClient.CreateIfNotExistsAsync(PublicAccessType.None);
                var ownersBlobClient = ownersContainerClient.GetBlobClient(ownersBlobName);

                var membersContainerClient = blobServiceClient.GetBlobContainerClient(CommunityMembersContainerName);
                await membersContainerClient.CreateIfNotExistsAsync(PublicAccessType.None);
                var membersBlobClient = membersContainerClient.GetBlobClient(membersBlobName);

                var parquetOptions = new ParquetOptions
                {
                    CompressionMethod = CompressionMethod.Snappy
                };

                // Open a stream to each of the blobs and create a ParquetWriter for each of them
                using var communitiesBlobStream = await communitiesBlobClient.OpenWriteAsync(overwrite: true);
                await using var communitiesWriter = await ParquetWriter.CreateAsync(communitySchema, communitiesBlobStream, parquetOptions);

                using var ownersBlobStream = await ownersBlobClient.OpenWriteAsync(overwrite: true);
                await using var ownersWriter = await ParquetWriter.CreateAsync(ownerSchema, ownersBlobStream, parquetOptions);

                using var membersBlobStream = await membersBlobClient.OpenWriteAsync(overwrite: true);
                await using var membersWriter = await ParquetWriter.CreateAsync(memberSchema, membersBlobStream, parquetOptions);

                int communitiesCount = 0;
                int ownersCount = 0; 
                int membersCount = 0;

                // Create the buffers and batch writes for communities, owners, and members
                var idBuffer = new List<string>(RowGroupBatchSize);
                var displayNameBuffer = new List<string>(RowGroupBatchSize);
                var sensitivityLabelIdBuffer = new List<string>(RowGroupBatchSize);
                var creationDateBuffer = new List<DateTime>(RowGroupBatchSize);
                var lastActivityDateBuffer = new List<DateTime>(RowGroupBatchSize);
                var communitySnapshotDateBuffer = new List<DateTime>(RowGroupBatchSize);

                async Task FlushCommunitiesBatchAsync()
                {
                    if (idBuffer.Count == 0)
                        return;

                    using var groupWriter = communitiesWriter.CreateRowGroup();
                    await groupWriter.WriteAsync(idField, idBuffer);
                    await groupWriter.WriteAsync(displayNameField, displayNameBuffer);
                    await groupWriter.WriteAsync(sensitivityLabelIdField, sensitivityLabelIdBuffer);
                    await groupWriter.WriteAsync<DateTime>(creationDateField, creationDateBuffer.ToArray().AsMemory());
                    await groupWriter.WriteAsync<DateTime>(lastActivityDateField, lastActivityDateBuffer.ToArray().AsMemory());
                    await groupWriter.WriteAsync<DateTime>(snapshotDateField, communitySnapshotDateBuffer.ToArray().AsMemory());

                    idBuffer.Clear();
                    displayNameBuffer.Clear();
                    sensitivityLabelIdBuffer.Clear();
                    creationDateBuffer.Clear();
                    lastActivityDateBuffer.Clear();
                    communitySnapshotDateBuffer.Clear();
                }

                var ownerUserIdBuffer = new List<string>(RowGroupBatchSize);
                var ownerCommunityIdBuffer = new List<string>(RowGroupBatchSize);
                var ownerSnapshotDateBuffer = new List<DateTime>(RowGroupBatchSize);

                async Task FlushOwnersBatchAsync()
                {
                    if (ownerUserIdBuffer.Count == 0)
                        return;

                    using var groupWriter = ownersWriter.CreateRowGroup();
                    await groupWriter.WriteAsync(communityIdField, ownerCommunityIdBuffer);
                    await groupWriter.WriteAsync(userIdField, ownerUserIdBuffer);
                    await groupWriter.WriteAsync<DateTime>(snapshotDateField, ownerSnapshotDateBuffer.ToArray().AsMemory());

                    ownerUserIdBuffer.Clear();
                    ownerCommunityIdBuffer.Clear();
                    ownerSnapshotDateBuffer.Clear();
                }

                var memberUserIdBuffer = new List<string>(RowGroupBatchSize);
                var memberCommunityIdBuffer = new List<string>(RowGroupBatchSize);
                var memberSnapshotDateBuffer = new List<DateTime>(RowGroupBatchSize);

                async Task FlushMembersBatchAsync()
                {
                    if (memberUserIdBuffer.Count == 0)
                        return;

                    using var groupWriter = membersWriter.CreateRowGroup();
                    await groupWriter.WriteAsync(communityIdField, memberCommunityIdBuffer);
                    await groupWriter.WriteAsync(userIdField, memberUserIdBuffer);
                    await groupWriter.WriteAsync<DateTime>(snapshotDateField, memberSnapshotDateBuffer.ToArray().AsMemory());

                    memberUserIdBuffer.Clear();
                    memberCommunityIdBuffer.Clear();
                    memberSnapshotDateBuffer.Clear();
                }

                // Get all groups with Teams provisioning 
                var groupsPage = await graph.Groups.GetAsync((requestConfiguration) =>
                {
                    requestConfiguration.Headers.Add("ConsistencyLevel", "eventual");
                    requestConfiguration.QueryParameters.Top = 999;
                    requestConfiguration.QueryParameters.Select = ["id", "createdDateTime", "displayName", "assignedLabels", "resourceProvisioningOptions"];
                    requestConfiguration.QueryParameters.Filter = "resourceProvisioningOptions/Any(x:x eq 'Team')";
                });

                var pageIterator = PageIterator<Group, GroupCollectionResponse>
                    .CreatePageIterator(
                        graph,
                        groupsPage!,
                        async group =>
                        {
                            if (group.Id != null && !exceptionGroupsArray.Contains(group.Id))
                            {
                                DateTime lastActivityDate = DateTime.MinValue;
                                var site = await graph.Groups[group.Id].Sites["root"].GetAsync();

                                // Find the team owner/members
                                var (owners, members) = await GetOwnersAndMembersAsync(graph, group.Id!, log);

                                // Check reports for last activity data
                                var teamsActivityRecord = teamsActivityRecords.FirstOrDefault(r => r.TeamId.Equals(group.Id));
                                var sharePointUsageRecord = site != null && site.Id != null ? sharepointUsageRecords.FirstOrDefault(r => r.SiteId.Equals(site.Id)) : new SharePointUsageRecord();

                                if (teamsActivityRecord != null && teamsActivityRecord.LastActivityDate != null)
                                    lastActivityDate = (DateTime)teamsActivityRecord.LastActivityDate;

                                if (sharePointUsageRecord != null && sharePointUsageRecord.LastActivityDate != null)
                                    lastActivityDate = lastActivityDate > (DateTime)sharePointUsageRecord.LastActivityDate ? lastActivityDate : (DateTime)sharePointUsageRecord.LastActivityDate;

                                // Add the data to be written to the Parquet files
                                idBuffer.Add(group.Id);
                                displayNameBuffer.Add(group.DisplayName ?? string.Empty);
                                sensitivityLabelIdBuffer.Add(group.AssignedLabels?.FirstOrDefault()?.LabelId ?? string.Empty);
                                creationDateBuffer.Add((group.CreatedDateTime ?? DateTimeOffset.MinValue).UtcDateTime);
                                lastActivityDateBuffer.Add(lastActivityDate);
                                communitySnapshotDateBuffer.Add(snapshotDate);

                                foreach (var owner in owners)
                                {
                                    ownerUserIdBuffer.Add(owner.Id);
                                    ownerCommunityIdBuffer.Add(group.Id);
                                    ownerSnapshotDateBuffer.Add(snapshotDate);
                                }

                                foreach (var member in members)
                                {
                                    memberUserIdBuffer.Add(member.Id);
                                    memberCommunityIdBuffer.Add(group.Id);
                                    memberSnapshotDateBuffer.Add(snapshotDate);
                                }

                                // Write to blob once the buffer reaches the batch size
                                if (idBuffer.Count >= RowGroupBatchSize)
                                    await FlushCommunitiesBatchAsync();

                                if (ownerUserIdBuffer.Count >= RowGroupBatchSize)
                                    await FlushOwnersBatchAsync();

                                if (memberUserIdBuffer.Count >= RowGroupBatchSize)
                                    await FlushMembersBatchAsync();

                                communitiesCount++;
                                ownersCount += owners.Length;
                                membersCount += members.Length;
                            }

                            return true;
                        },
                        requestConfiguration =>
                        {
                            requestConfiguration.Headers.Add("ConsistencyLevel", "eventual");
                            return requestConfiguration;
                        });

                await pageIterator.IterateAsync();

                await FlushCommunitiesBatchAsync();
                await FlushOwnersBatchAsync();
                await FlushMembersBatchAsync();

                await communitiesWriter.DisposeAsync();
                await ownersWriter.DisposeAsync();
                await membersWriter.DisposeAsync();

                log.LogInformation($"Streamed {communitiesCount} communities to {communitiesBlobName}");
                log.LogInformation($"Streamed {ownersCount} owners to {ownersBlobName}");
                log.LogInformation($"Streamed {membersCount} members to {membersBlobName}");

                return communitiesBlobName;
            }
            catch (Exception ex) 
            {
                log.LogError("StreamCommunitiesToBlobAsync failed.");
                log.LogError(ex.Message.ToString());
            }

            return string.Empty;
        }

        private static async Task<(UserRecord[] Owners, UserRecord[] Members)> GetOwnersAndMembersAsync(GraphServiceClient graph, string groupId, ILogger log)
        {
            try
            {
                var ownersTask = GetDirectoryObjectPageAsUsersAsync(
                    () => graph.Groups[groupId].Owners.GetAsync(rc =>
                    {
                        rc.Headers.Add("ConsistencyLevel", "eventual");
                        rc.QueryParameters.Top = 999;
                        rc.QueryParameters.Select = Users.UserQuerySelectParams;
                    }),
                    graph);

                var membersTask = GetDirectoryObjectPageAsUsersAsync(
                    () => graph.Groups[groupId].Members.GetAsync(rc =>
                    {
                        rc.Headers.Add("ConsistencyLevel", "eventual");
                        rc.QueryParameters.Top = 999;
                        rc.QueryParameters.Select = Users.UserQuerySelectParams;
                    }),
                    graph);

                await Task.WhenAll(ownersTask, membersTask);

                return (ownersTask.Result, membersTask.Result);
            }
            catch (Exception ex)
            {
                log.LogWarning(ex, "Failed to fetch owners/members for group {GroupId}", groupId);
                return (Array.Empty<UserRecord>(), Array.Empty<UserRecord>());
            }
        }

        private static async Task<UserRecord[]> GetDirectoryObjectPageAsUsersAsync(Func<Task<DirectoryObjectCollectionResponse?>> initialRequest, GraphServiceClient graph)
        {
            var results = new List<UserRecord>();
            var page = await initialRequest();

            var iterator = PageIterator<DirectoryObject, DirectoryObjectCollectionResponse>
                .CreatePageIterator(
                    graph,
                    page!,
                    directoryObject =>
                    {
                        if (directoryObject is User user)
                        {
                            results.Add(new UserRecord(
                                Id: user.Id ?? string.Empty,
                                Mail: user.Mail ?? string.Empty
                            ));
                        }
                        // service principals owning a team are skipped.
                        return true;
                    });

            await iterator.IterateAsync();
            return results.ToArray();
        }
    }
}
