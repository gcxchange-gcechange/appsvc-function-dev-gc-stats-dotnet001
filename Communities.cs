using Azure.Identity;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Graph;
using Microsoft.Graph.Models;
using System.Text.Json;

namespace GCStats
{
    public record CommunityRecord(
        string Id, 
        string DisplayName, 
        UserRecord[] OwnerList,
        UserRecord[] MemberList,
        List<string> GroupTypes,
        SensitivityLabelRecord SensitivityLabel,
        DateTimeOffset CreationDate,
        DateTimeOffset LastActivityDate
    );

    public record SensitivityLabelRecord(string Id, string DisplayName);

    static class Communities
    {
        public static async Task StreamCommunitiesToBlobAsync(ILogger log, IConfiguration config)
        {
            var storageAccountUrl = config["storageAccountUrl"];
            var isLocal = config["isLocal"];
            var containerName = "communities";
            var blobName = $"communities-{DateTime.UtcNow:yyyy/MM/dd}.json";

            var blobServiceClient = new BlobServiceClient(new Uri(storageAccountUrl), isLocal == "true" ? new AzureCliCredential() : new DefaultAzureCredential());
            var containerClient = blobServiceClient.GetBlobContainerClient(containerName);
            await containerClient.CreateIfNotExistsAsync(PublicAccessType.None);
            var blobClient = containerClient.GetBlobClient(blobName);

            using var blobStream = await blobClient.OpenWriteAsync(overwrite: true);
            using var jsonWriter = new Utf8JsonWriter(blobStream);

            jsonWriter.WriteStartArray();

            var graph = new Auth().GraphAuth(log);
            int count = 0;

            var groupsPage = await graph.Groups.GetAsync((requestConfiguration) =>
            {
                requestConfiguration.Headers.Add("ConsistencyLevel", "eventual");
                requestConfiguration.QueryParameters.Top = 999;
                requestConfiguration.QueryParameters.Select = ["id", "createdDateTime", "displayName", "groupTypes", "assignedLabels", "resourceProvisioningOptions"];
                requestConfiguration.QueryParameters.Filter = "resourceProvisioningOptions/Any(x:x eq 'Team')";
            });

            var pageIterator = PageIterator<Group, GroupCollectionResponse>
                .CreatePageIterator(
                    graph,
                    groupsPage!,
                    async group =>
                    {
                        var (owners, members) = await GetOwnersAndMembersAsync(graph, group.Id!, log);

                        var record = new CommunityRecord(
                            Id: group.Id ?? string.Empty,
                            DisplayName: group.DisplayName ?? string.Empty,
                            OwnerList: owners,
                            MemberList: members,
                            GroupTypes: group.GroupTypes ?? new List<string>(),
                            SensitivityLabel: new SensitivityLabelRecord(
                                Id: group.AssignedLabels?.FirstOrDefault()?.LabelId ?? string.Empty,
                                DisplayName: group.AssignedLabels?.FirstOrDefault()?.DisplayName ?? string.Empty
                            ),
                            CreationDate: group.CreatedDateTime ?? DateTime.MinValue,
                            LastActivityDate: DateTime.MinValue // TODO
                        );

                        JsonSerializer.Serialize(jsonWriter, record, Globals.JsonOptions);
                        count++;
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
        }

        private static async Task<(UserRecord[] Owners, UserRecord[] Members)> GetOwnersAndMembersAsync(GraphServiceClient graph, string groupId, ILogger log)
        {
            try
            {
                var ownersTask = GetDirectoryObjectPageAsUsersAsync(
                    () => graph.Groups[groupId].Owners.GetAsync(rc =>
                    {
                        rc.QueryParameters.Top = 999;
                        rc.QueryParameters.Select = ["id", "mail"];
                    }),
                    graph);

                var membersTask = GetDirectoryObjectPageAsUsersAsync(
                    () => graph.Groups[groupId].Members.GetAsync(rc =>
                    {
                        rc.QueryParameters.Top = 999;
                        rc.QueryParameters.Select = ["id", "mail"];
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
