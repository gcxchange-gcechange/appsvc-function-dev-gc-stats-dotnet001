using Azure.Identity;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using CsvHelper;
using GCStats.Models;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Graph;
using Microsoft.Graph.Models;
using System.Globalization;
using System.Text.Json;

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

                // Open stream to blob storage
                var storageAccountUrl = Globals.GetAppSetting("storageAccountUrl", log, config);
                var exceptionGroupsArray = Globals.GetAppSetting("exceptionGroupsArray", log, config);
                var isLocal = Globals.GetAppSetting("isLocal", log, config, false);
                var blobName = $"communities-{DateTime.UtcNow:yyyy-MM-dd}.json";

                var blobServiceClient = new BlobServiceClient(new Uri(storageAccountUrl), isLocal == "true" ? new AzureCliCredential() : new DefaultAzureCredential());
                var containerClient = blobServiceClient.GetBlobContainerClient(TotalCommunitiesContainerName);
                await containerClient.CreateIfNotExistsAsync(PublicAccessType.None);
                var blobClient = containerClient.GetBlobClient(blobName);

                using var blobStream = await blobClient.OpenWriteAsync(overwrite: true);
                using var jsonWriter = new Utf8JsonWriter(blobStream);

                jsonWriter.WriteStartArray();
                int count = 0;

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
                                var sharePointUsageRecord = site != null && site.Id != null ?
                                    sharepointUsageRecords.FirstOrDefault(r => r.SiteId.Equals(site.Id)) :
                                    new SharePointUsageRecord();

                                if (teamsActivityRecord != null && teamsActivityRecord.LastActivityDate != null)
                                    lastActivityDate = (DateTime)teamsActivityRecord.LastActivityDate;
                                else
                                    log.LogWarning($"Coudln't find teams activity for GroupId: {group.Id}");

                                if (sharePointUsageRecord != null && sharePointUsageRecord.LastActivityDate != null)
                                    lastActivityDate = lastActivityDate > (DateTime)sharePointUsageRecord.LastActivityDate ? 
                                    lastActivityDate : (DateTime)sharePointUsageRecord.LastActivityDate;
                                else
                                    log.LogWarning($"Couldn't find SharePoint site activity for GroupId: {group.Id}");

                                // Write to blob storage
                                var record = new CommunityRecord(
                                    Id: group.Id,
                                    DisplayName: group.DisplayName ?? string.Empty,
                                    OwnerList: owners,
                                    MemberList: members,
                                    SensitivityLabel: new SensitivityLabelRecord(
                                        Id: group.AssignedLabels?.FirstOrDefault()?.LabelId ?? string.Empty,
                                        DisplayName: group.AssignedLabels?.FirstOrDefault()?.DisplayName ?? string.Empty
                                    ),
                                    CreationDate: group.CreatedDateTime ?? DateTime.MinValue,
                                    LastActivityDate: lastActivityDate
                                );

                                JsonSerializer.Serialize(jsonWriter, record, Globals.JsonOptions);

                                log.LogInformation($"Processed community #{++count}: {record.DisplayName} (ID: {record.Id})");
                            }

                            return true;
                        },
                        requestConfiguration =>
                        {
                            requestConfiguration.Headers.Add("ConsistencyLevel", "eventual");
                            return requestConfiguration;
                        });

                await pageIterator.IterateAsync();

                jsonWriter.WriteEndArray();
                await jsonWriter.FlushAsync();
                await blobStream.FlushAsync();

                log.LogInformation("Streamed {Count} communities to blob {BlobName}", count, blobName);

                return blobName;
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
