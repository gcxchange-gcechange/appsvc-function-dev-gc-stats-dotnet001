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
    public record UserRecord(string Id, string Mail);

    static class Users
    {
        public static readonly string[] UserQuerySelectParams = ["id", "mail"];

        public const string TotalUsersContainerName = "users";

        public static async Task<string> StreamUsersToBlobAsync(ILogger log, IConfiguration config)
        {
            try
            {
                var storageAccountUrl = Globals.GetAppSetting("storageAccountUrl", log, config);
                var exceptionUsersArray = Globals.GetAppSetting("exceptionUsersArray", log, config);
                var isLocal = Globals.GetAppSetting("isLocal", log, config, false);
                var blobName = $"users-{DateTime.UtcNow:yyyy-MM-dd}.json";

                var blobServiceClient = new BlobServiceClient(new Uri(storageAccountUrl), isLocal == "true" ? new AzureCliCredential() : new DefaultAzureCredential());
                var containerClient = blobServiceClient.GetBlobContainerClient(TotalUsersContainerName);
                await containerClient.CreateIfNotExistsAsync(PublicAccessType.None);
                var blobClient = containerClient.GetBlobClient(blobName);

                using var blobStream = await blobClient.OpenWriteAsync(overwrite: true);
                using var jsonWriter = new Utf8JsonWriter(blobStream);

                jsonWriter.WriteStartArray();

                var graph = Auth.GraphAuth(log);
                int count = 0;

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
                                var record = new UserRecord(Id: user.Id, Mail: user.Mail ?? string.Empty);
                                JsonSerializer.Serialize(jsonWriter, record, Globals.JsonOptions);
                                count++;
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

                log.LogInformation("Streamed {Count} users to blob {BlobName}", count, blobName);

                return blobName;
            }
            catch (Exception ex)
            {
                log.LogError("StreamUsersToBlobAsync failed");
                log.LogError(ex.Message);
            }

            return string.Empty;
        }
    }
}
