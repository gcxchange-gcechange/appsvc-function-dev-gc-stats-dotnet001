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
        private const int ReportDateRangeInDays = 7;

        public static async Task<string> StreamCommunitiesToBlobAsync(ILogger log, IConfiguration config, CancellationToken ct = default)
        {
            var communitiesBlobName = $"{TotalCommunitiesContainerName}-{DateTime.UtcNow.ToString(Globals.BlobDateFormat)}.parquet";
            var ownersBlobName = $"{CommunityOwnersContainerName}-{DateTime.UtcNow.ToString(Globals.BlobDateFormat)}.parquet";
            var membersBlobName = $"{CommunityMembersContainerName}-{DateTime.UtcNow.ToString(Globals.BlobDateFormat)}.parquet";

            try
            {
                var graph = Auth.GetGraphServiceClient(log);

                // Get teams activity report
                using var teamsUsageStream = await graph.Reports.GetTeamsTeamActivityDetailWithPeriod($"D{ReportDateRangeInDays}").GetAsync(cancellationToken: ct);
                using var teamsUsageReader = new StreamReader(teamsUsageStream);
                using var teamsUsage = new CsvReader(teamsUsageReader, CultureInfo.InvariantCulture);
                var teamsActivityRecords = teamsUsage.GetRecords<TeamActivityRecord>().ToList();

                // Get sharepoint usage report
                using var sharepointUsageStream = await graph.Reports.GetSharePointSiteUsageDetailWithPeriod($"D{ReportDateRangeInDays}").GetAsync(cancellationToken: ct);
                using var sharepointUsageReader = new StreamReader(sharepointUsageStream);
                using var sharepointUsage = new CsvReader(sharepointUsageReader, CultureInfo.InvariantCulture);
                var sharepointUsageRecords = sharepointUsage.GetRecords<SharePointUsageRecord>().ToList();

                // Get the directory audits for group membership changes
                var membershipChangeDates = await GetGroupMembershipChangeDatesAsync(graph, log, ct);

                var storageAccountUrl = Auth.GetAppSetting("storageAccountUrl", log, config);
                var exceptionGroupsArray = Auth.GetAppSetting("exceptionGroupsArray", log, config);
                var isLocal = Auth.GetAppSetting("isLocal", log, config, false);

                var snapshotDate = DateTime.UtcNow.Date;

                // TotalCommunities Fields
                var idField = new DataField<string>("Id");
                var siteIdField = new DataField<string>("SiteId");
                var displayNameField = new DataField<string>("DisplayName");
                var webUrlField = new DataField<string>("WebUrl");
                var sensitivityLabelIdField = new DataField<string>("SensitivityLabelId");
                var creationDateField = new DataField<DateTime>("CreationDate");
                var lastActivityDateField = new DataField<DateTime>("LastActivityDate");
                var lastActivityDateSharePointField = new DataField<DateTime?>("LastActivityDateSharePoint");
                var lastActivityDateTeamsField = new DataField<DateTime?>("LastActivityDateTeams");
                var lastActivityDateMembershipChangeField = new DataField<DateTime?>("LastActivityDateMembershipChange");
                var ownerCountField = new DataField<int>("OwnerCount");
                var memberCountField = new DataField<int>("MemberCount");
                var visibilityField = new DataField<string>("Visibility");

                // Community Owner/Member Fields
                var communityIdField = new DataField<string>("CommunityId");
                var userIdField = new DataField<string>("UserId");

                // Shared Fields
                var snapshotDateField = new DataField<DateTime>("SnapshotDate");

                var communitySchema = new ParquetSchema(idField, siteIdField, displayNameField, webUrlField, sensitivityLabelIdField, 
                    creationDateField, lastActivityDateField, lastActivityDateSharePointField, lastActivityDateTeamsField, 
                    lastActivityDateMembershipChangeField, ownerCountField, memberCountField, visibilityField, snapshotDateField);

                var ownerSchema = new ParquetSchema(communityIdField, userIdField, snapshotDateField);
                var memberSchema = new ParquetSchema(communityIdField, userIdField, snapshotDateField);

                var communitiesBlobClient = await Auth.GetBlobClient(TotalCommunitiesContainerName, communitiesBlobName, log, config);
                var ownersBlobClient = await Auth.GetBlobClient(CommunityOwnersContainerName, ownersBlobName, log, config);
                var membersBlobClient = await Auth.GetBlobClient(CommunityMembersContainerName, membersBlobName, log, config);

                // Open a stream to each of the blobs and create a ParquetWriter for each of them
                using var communitiesBlobStream = await communitiesBlobClient.OpenWriteAsync(overwrite: true, cancellationToken: ct);
                await using var communitiesWriter = await ParquetWriter.CreateAsync(communitySchema, communitiesBlobStream, Globals.ParquetOptions, cancellationToken: ct);

                using var ownersBlobStream = await ownersBlobClient.OpenWriteAsync(overwrite: true, cancellationToken: ct);
                await using var ownersWriter = await ParquetWriter.CreateAsync(ownerSchema, ownersBlobStream, Globals.ParquetOptions, cancellationToken: ct);

                using var membersBlobStream = await membersBlobClient.OpenWriteAsync(overwrite: true, cancellationToken: ct);
                await using var membersWriter = await ParquetWriter.CreateAsync(memberSchema, membersBlobStream, Globals.ParquetOptions, cancellationToken: ct);

                int communitiesCount = 0;
                int ownersCount = 0; 
                int membersCount = 0;

                // Create the buffers and batch writes for communities, owners, and members
                var idBuffer = new List<string>(Globals.RowGroupBatchSize);
                var siteIdBuffer = new List<string>(Globals.RowGroupBatchSize);
                var displayNameBuffer = new List<string>(Globals.RowGroupBatchSize);
                var webUrlBuffer = new List<string>(Globals.RowGroupBatchSize);
                var sensitivityLabelIdBuffer = new List<string>(Globals.RowGroupBatchSize);
                var creationDateBuffer = new List<DateTime>(Globals.RowGroupBatchSize);
                var lastActivityDateBuffer = new List<DateTime>(Globals.RowGroupBatchSize);
                var lastActivityDateSharePointBuffer = new List<DateTime?>(Globals.RowGroupBatchSize);
                var lastActivityDateTeamsBuffer = new List<DateTime?>(Globals.RowGroupBatchSize);
                var lastActivityDateMembershipChangeBuffer = new List<DateTime?>(Globals.RowGroupBatchSize);
                var ownerCountBuffer = new List<int>(Globals.RowGroupBatchSize);
                var memberCountBuffer = new List<int>(Globals.RowGroupBatchSize);
                var visibilityBuffer = new List<string>(Globals.RowGroupBatchSize);
                var communitySnapshotDateBuffer = new List<DateTime>(Globals.RowGroupBatchSize);

                async Task FlushCommunitiesBatchAsync()
                {
                    ct.ThrowIfCancellationRequested();

                    if (idBuffer.Count == 0)
                        return;

                    using var groupWriter = communitiesWriter.CreateRowGroup();
                    await groupWriter.WriteAsync(idField, idBuffer);
                    await groupWriter.WriteAsync(siteIdField, siteIdBuffer);
                    await groupWriter.WriteAsync(displayNameField, displayNameBuffer);
                    await groupWriter.WriteAsync(webUrlField, webUrlBuffer);
                    await groupWriter.WriteAsync(sensitivityLabelIdField, sensitivityLabelIdBuffer);
                    await groupWriter.WriteAsync<DateTime>(creationDateField, creationDateBuffer.ToArray().AsMemory(), cancellationToken: ct);
                    await groupWriter.WriteAsync<DateTime>(lastActivityDateField, lastActivityDateBuffer.ToArray().AsMemory(), cancellationToken: ct);
                    await groupWriter.WriteAsync<DateTime>(lastActivityDateSharePointField, lastActivityDateSharePointBuffer.ToArray().AsMemory(), cancellationToken: ct);
                    await groupWriter.WriteAsync<DateTime>(lastActivityDateTeamsField, lastActivityDateTeamsBuffer.ToArray().AsMemory(), cancellationToken: ct);
                    await groupWriter.WriteAsync<DateTime>(lastActivityDateMembershipChangeField, lastActivityDateMembershipChangeBuffer.ToArray().AsMemory(), cancellationToken: ct);
                    await groupWriter.WriteAsync<int>(ownerCountField, ownerCountBuffer.ToArray().AsMemory(), cancellationToken: ct);
                    await groupWriter.WriteAsync<int>(memberCountField, memberCountBuffer.ToArray().AsMemory(), cancellationToken: ct);
                    await groupWriter.WriteAsync(visibilityField, visibilityBuffer);
                    await groupWriter.WriteAsync<DateTime>(snapshotDateField, communitySnapshotDateBuffer.ToArray().AsMemory(), cancellationToken: ct);

                    idBuffer.Clear();
                    siteIdBuffer.Clear();
                    displayNameBuffer.Clear();
                    webUrlBuffer.Clear();
                    sensitivityLabelIdBuffer.Clear();
                    creationDateBuffer.Clear();
                    lastActivityDateBuffer.Clear();
                    lastActivityDateSharePointBuffer.Clear();
                    lastActivityDateTeamsBuffer.Clear();
                    lastActivityDateMembershipChangeBuffer.Clear();
                    ownerCountBuffer.Clear();
                    memberCountBuffer.Clear();
                    visibilityBuffer.Clear();
                    communitySnapshotDateBuffer.Clear();
                }

                var ownerUserIdBuffer = new List<string>(Globals.RowGroupBatchSize);
                var ownerCommunityIdBuffer = new List<string>(Globals.RowGroupBatchSize);
                var ownerSnapshotDateBuffer = new List<DateTime>(Globals.RowGroupBatchSize);

                async Task FlushOwnersBatchAsync()
                {
                    ct.ThrowIfCancellationRequested();

                    if (ownerUserIdBuffer.Count == 0)
                        return;

                    using var groupWriter = ownersWriter.CreateRowGroup();
                    await groupWriter.WriteAsync(communityIdField, ownerCommunityIdBuffer);
                    await groupWriter.WriteAsync(userIdField, ownerUserIdBuffer);
                    await groupWriter.WriteAsync<DateTime>(snapshotDateField, ownerSnapshotDateBuffer.ToArray().AsMemory(), cancellationToken: ct);

                    ownerUserIdBuffer.Clear();
                    ownerCommunityIdBuffer.Clear();
                    ownerSnapshotDateBuffer.Clear();
                }

                var memberUserIdBuffer = new List<string>(Globals.RowGroupBatchSize);
                var memberCommunityIdBuffer = new List<string>(Globals.RowGroupBatchSize);
                var memberSnapshotDateBuffer = new List<DateTime>(Globals.RowGroupBatchSize);

                async Task FlushMembersBatchAsync()
                {
                    ct.ThrowIfCancellationRequested();

                    if (memberUserIdBuffer.Count == 0)
                        return;

                    using var groupWriter = membersWriter.CreateRowGroup();
                    await groupWriter.WriteAsync(communityIdField, memberCommunityIdBuffer);
                    await groupWriter.WriteAsync(userIdField, memberUserIdBuffer);
                    await groupWriter.WriteAsync<DateTime>(snapshotDateField, memberSnapshotDateBuffer.ToArray().AsMemory(), cancellationToken: ct);

                    memberUserIdBuffer.Clear();
                    memberCommunityIdBuffer.Clear();
                    memberSnapshotDateBuffer.Clear();
                }

                // Get all groups with Teams provisioning 
                var groupsPage = await graph.Groups.GetAsync((requestConfiguration) =>
                {
                    requestConfiguration.Headers.Add("ConsistencyLevel", "eventual");
                    requestConfiguration.QueryParameters.Top = 999;
                    requestConfiguration.QueryParameters.Select = ["id", "createdDateTime", "displayName", "assignedLabels", "resourceProvisioningOptions", "visibility"];
                    requestConfiguration.QueryParameters.Filter = "resourceProvisioningOptions/Any(x:x eq 'Team')";
                }, ct);

                var failedGroupIds = new List<string>();

                var pageIterator = PageIterator<Group, GroupCollectionResponse>
                    .CreatePageIterator(
                        graph,
                        groupsPage!,
                        async group =>
                        {
                            ct.ThrowIfCancellationRequested();

                            if (group.Id != null && !exceptionGroupsArray.Contains(group.Id))
                            {
                                var lastActivityDate = group.CreatedDateTime?.UtcDateTime ?? DateTime.MinValue;
                                Site? site = null;

                                try
                                {
                                    site = await graph.Groups[group.Id].Sites["root"].GetAsync(cancellationToken: ct);
                                }
                                catch (Microsoft.Graph.Models.ODataErrors.ODataError ex) when (ex.ResponseStatusCode == 404)
                                {
                                    log.LogWarning($"No root site for group {group.Id}");
                                }

                                // Find the team owner/members
                                var ownersAndMembers = await GetOwnersAndMembersAsync(graph, group.Id!, log, ct);

                                // If we failed to get any members/owners then we skip writing this row. 
                                if (ownersAndMembers is null)
                                {
                                    failedGroupIds.Add(group.Id);
                                    log.LogWarning($"Failed to get owners/members for group {group.Id}, skipping this group.");
                                    return true;
                                }

                                var (owners, members) = ownersAndMembers!.Value;

                                // Check reports for last activity data
                                var teamsActivityRecord = teamsActivityRecords.FirstOrDefault(r => r.TeamId.Equals(group.Id, StringComparison.OrdinalIgnoreCase));

                                var siteIdGuid = site?.Id?.Split(',').ElementAtOrDefault(1);
                                var sharePointUsageRecord = siteIdGuid != null ? sharepointUsageRecords.FirstOrDefault(r => r.SiteId.Equals(siteIdGuid, StringComparison.OrdinalIgnoreCase)) : new SharePointUsageRecord();

                                DateTime? lastActivitySP = null;
                                DateTime? lastActivityTeams = null;
                                DateTime? lastActivityMembership = null;

                                if (teamsActivityRecord != null && teamsActivityRecord.LastActivityDate != null)
                                {
                                    lastActivityDate = (DateTime)teamsActivityRecord.LastActivityDate;
                                    lastActivityTeams = lastActivityDate;
                                }
                                    

                                if (sharePointUsageRecord != null && sharePointUsageRecord.LastActivityDate != null)
                                {
                                    lastActivitySP = (DateTime)sharePointUsageRecord.LastActivityDate;

                                    if (lastActivitySP > lastActivityDate)
                                        lastActivityDate = (DateTime)lastActivitySP;
                                }
                                    

                                if (membershipChangeDates.TryGetValue(group.Id, out var membershipChangeDate))
                                {
                                    lastActivityMembership = membershipChangeDate;

                                    if (membershipChangeDate > lastActivityDate)
                                        lastActivityDate = membershipChangeDate;
                                }

                                // Add the data to be written to the Parquet files
                                idBuffer.Add(group.Id);
                                siteIdBuffer.Add(site?.Id ?? string.Empty);
                                displayNameBuffer.Add(group.DisplayName ?? string.Empty);
                                webUrlBuffer.Add(site?.WebUrl ?? string.Empty);
                                sensitivityLabelIdBuffer.Add(group.AssignedLabels?.FirstOrDefault()?.LabelId ?? string.Empty);
                                creationDateBuffer.Add((group.CreatedDateTime ?? DateTimeOffset.MinValue).UtcDateTime);
                                lastActivityDateBuffer.Add(lastActivityDate);
                                lastActivityDateSharePointBuffer.Add(lastActivitySP);
                                lastActivityDateTeamsBuffer.Add(lastActivityTeams);
                                lastActivityDateMembershipChangeBuffer.Add(lastActivityMembership ?? null);
                                ownerCountBuffer.Add(owners.Length);
                                memberCountBuffer.Add(members.Length);
                                visibilityBuffer.Add(group.Visibility ?? string.Empty);
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
                                if (idBuffer.Count >= Globals.RowGroupBatchSize)
                                    await FlushCommunitiesBatchAsync();

                                if (ownerUserIdBuffer.Count >= Globals.RowGroupBatchSize)
                                    await FlushOwnersBatchAsync();

                                if (memberUserIdBuffer.Count >= Globals.RowGroupBatchSize)
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

                await pageIterator.IterateAsync(ct);


                if (failedGroupIds.Count > 0)
                {
                    var threshold = Math.Max(5, (int)((communitiesCount + failedGroupIds.Count) * 0.01));

                    if (failedGroupIds.Count > threshold)
                        throw new InvalidOperationException($"Too many groups failed ({failedGroupIds.Count}) aborting so a bad snapshot isn't published.");
                }

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
            catch (OperationCanceledException)
            {
                log.LogWarning("StreamCommunitiesToBlobAsync was cancelled");

                await Globals.DeletePartialBlob(TotalCommunitiesContainerName, communitiesBlobName, log, config);
                await Globals.DeletePartialBlob(CommunityOwnersContainerName, ownersBlobName, log, config);
                await Globals.DeletePartialBlob(CommunityMembersContainerName, membersBlobName, log, config);

                throw;
            }
            catch (Exception ex) 
            {
                log.LogError("StreamCommunitiesToBlobAsync failed.");
                log.LogError(ex.Message.ToString());

                await Globals.DeletePartialBlob(TotalCommunitiesContainerName, communitiesBlobName, log, config);
                await Globals.DeletePartialBlob(CommunityOwnersContainerName, ownersBlobName, log, config);
                await Globals.DeletePartialBlob(CommunityMembersContainerName, membersBlobName, log, config);

                throw;
            }
        }

        private static async Task<(UserRecord[] Owners, UserRecord[] Members)?> GetOwnersAndMembersAsync(GraphServiceClient graph, string groupId, ILogger log, CancellationToken ct = default)
        {
            try
            {
                var ownersTask = GetDirectoryObjectPageAsUsersAsync(
                    () => graph.Groups[groupId].Owners.GetAsync(rc =>
                    {
                        rc.Headers.Add("ConsistencyLevel", "eventual");
                        rc.QueryParameters.Top = 999;
                        rc.QueryParameters.Select = Users.UserQuerySelectParams;
                    }, ct),
                    graph, ct);

                var membersTask = GetDirectoryObjectPageAsUsersAsync(
                    () => graph.Groups[groupId].Members.GetAsync(rc =>
                    {
                        rc.Headers.Add("ConsistencyLevel", "eventual");
                        rc.QueryParameters.Top = 999;
                        rc.QueryParameters.Select = Users.UserQuerySelectParams;
                    }, ct),
                    graph, ct);

                await Task.WhenAll(ownersTask, membersTask);

                return (ownersTask.Result, membersTask.Result);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                log.LogWarning(ex, $"Failed to fetch owners/members for group {groupId}");
                return null;
            }
        }

        private static async Task<UserRecord[]> GetDirectoryObjectPageAsUsersAsync(Func<Task<DirectoryObjectCollectionResponse?>> initialRequest, GraphServiceClient graph, CancellationToken ct = default)
        {
            var results = new List<UserRecord>();
            var page = await initialRequest();

            var iterator = PageIterator<DirectoryObject, DirectoryObjectCollectionResponse>
                .CreatePageIterator(
                    graph,
                    page!,
                    directoryObject =>
                    {
                        ct.ThrowIfCancellationRequested();

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

            await iterator.IterateAsync(ct);
            return results.ToArray();
        }

        private static async Task<Dictionary<string, DateTime>> GetGroupMembershipChangeDatesAsync(GraphServiceClient graph, ILogger log, CancellationToken ct = default)
        {
            var result = new Dictionary<string, DateTime>();

            var windowStart = DateTime.UtcNow.Date.AddDays(-ReportDateRangeInDays);
            var windowEnd = DateTime.UtcNow.Date;

            var auditsPage = await graph.AuditLogs.DirectoryAudits.GetAsync(rc =>
            {
                rc.QueryParameters.Filter = $"category eq 'GroupManagement' and activityDateTime ge {windowStart:o} and activityDateTime lt {windowEnd:o}";
                rc.QueryParameters.Top = 999;
                rc.QueryParameters.Select = ["activityDisplayName", "activityDateTime", "targetResources"];
            }, ct);

            if (auditsPage is null)
                return result;

            string[] targetedActivity = 
            {
                "Add member to group",
                "Remove member from group",
                "Add owner to group",
                "Remove owner from group"
            };

            var iterator = PageIterator<DirectoryAudit, DirectoryAuditCollectionResponse>
                .CreatePageIterator(
                    graph,
                    auditsPage!,
                    audit =>
                    {
                        if (targetedActivity.Contains(audit.ActivityDisplayName) && audit.ActivityDateTime.HasValue)
                        {
                            // The group itself is one of the TargetResources (type "Group")
                            var groupTarget = audit.TargetResources?.FirstOrDefault(t => t.Type == "Group");

                            if (groupTarget?.Id != null)
                            {
                                var changeDate = audit.ActivityDateTime.Value.UtcDateTime;

                                if (!result.TryGetValue(groupTarget.Id, out var existing) || changeDate > existing)
                                    result[groupTarget.Id] = changeDate;
                            }
                        }

                        return true;
                    });

            await iterator.IterateAsync(ct);
            return result;
        }
    }
}
