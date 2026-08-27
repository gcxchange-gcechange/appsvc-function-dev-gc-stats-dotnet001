using Azure.Identity;
using Azure.Storage.Blobs;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System.Data;
using System.Text.Json;

namespace GCStats
{
    public class ProcessTotalCommunities
    {
        private readonly ILogger<ProcessTotalCommunities> _logger;
        private readonly IConfiguration _config;

        public ProcessTotalCommunities(ILogger<ProcessTotalCommunities> logger, IConfiguration config)
        {
            _logger = logger;
            _config = config;
        }

        [Function("ProcessTotalCommunities")]
        public async Task Run([QueueTrigger("process-total-communities", Connection = "AzureWebJobsStorage")] string blobName)
        {
            _logger.LogInformation("Received blobName: {blobName}", blobName);

            try
            {
                var storageAccountUrl = Globals.GetAppSetting("storageAccountUrl", _logger, _config);
                var isLocal = Globals.GetAppSetting("isLocal", _logger, _config, false);

                var blobServiceClient = new BlobServiceClient(new Uri(storageAccountUrl), isLocal == "true" ? new AzureCliCredential() : new DefaultAzureCredential());
                var containerClient = blobServiceClient.GetBlobContainerClient(Communities.TotalCommunitiesContainerName);
                var blobClient = containerClient.GetBlobClient(blobName);

                var response = await blobClient.DownloadContentAsync();
                var communities = JsonSerializer.Deserialize<List<CommunityRecord>>(response.Value.Content.ToString(), new JsonSerializerOptions { PropertyNameCaseInsensitive = true });

                if (communities != null && communities.Count > 0)
                {
                    _logger.LogInformation("Processing {count} communities from blob: {blobName}", communities.Count, blobName);

                    var communityDataTable = new DataTable();
                    communityDataTable.Columns.Add("Id", typeof(string));
                    communityDataTable.Columns.Add("DisplayName", typeof(string));
                    communityDataTable.Columns.Add("SensitivityLabelId", typeof(string));
                    communityDataTable.Columns.Add("CreationDate", typeof(DateTime));
                    communityDataTable.Columns.Add("LastActivityDate", typeof(DateTime));
                    communityDataTable.Columns.Add("SnapshotDate", typeof(DateTime));

                    var ownerDataTable = new DataTable();
                    ownerDataTable.Columns.Add("Id", typeof(string));
                    ownerDataTable.Columns.Add("CommunityId", typeof(string));
                    ownerDataTable.Columns.Add("SnapshotDate", typeof(DateTime));

                    var memberDataTable = new DataTable();
                    memberDataTable.Columns.Add("Id", typeof(string));
                    memberDataTable.Columns.Add("CommunityId", typeof(string));
                    memberDataTable.Columns.Add("SnapshotDate", typeof(DateTime));

                    var splitBlobName = blobName.Split('.').First().Split('-').Skip(1);
                    var snapshotDate = DateTime.Parse(String.Join("-", splitBlobName));

                    foreach (var community in communities)
                    {
                        communityDataTable.Rows.Add(community.Id, community.DisplayName, community.SensitivityLabel.Id, community.CreationDate.UtcDateTime, community.LastActivityDate.UtcDateTime, snapshotDate);

                        foreach (var owner in community.OwnerList)
                        {
                            ownerDataTable.Rows.Add(owner.Id, community.Id, snapshotDate);
                        }

                        foreach (var member in community.MemberList)
                        {
                            memberDataTable.Rows.Add(member.Id, community.Id, snapshotDate);
                        }
                    }

                    using var sqlConnection = await Auth.GetSqlConnection(_logger, _config);
                    using var transaction = sqlConnection.BeginTransaction();

                    try
                    {
                        var bulkCopyOptions = SqlBulkCopyOptions.TableLock;

                        using (var bulkCopy = new SqlBulkCopy(sqlConnection, bulkCopyOptions, transaction))
                        {
                            bulkCopy.DestinationTableName = "dbo.TotalCommunities";
                            bulkCopy.BatchSize = 50000;
                            bulkCopy.BulkCopyTimeout = 0;

                            bulkCopy.ColumnMappings.Add("Id", "Id");
                            bulkCopy.ColumnMappings.Add("DisplayName", "DisplayName");
                            bulkCopy.ColumnMappings.Add("SensitivityLabelId", "SensitivityLabelId");
                            bulkCopy.ColumnMappings.Add("CreationDate", "CreationDate");
                            bulkCopy.ColumnMappings.Add("LastActivityDate", "LastActivityDate");
                            bulkCopy.ColumnMappings.Add("SnapshotDate", "SnapshotDate");

                            await bulkCopy.WriteToServerAsync(communityDataTable);
                            _logger.LogInformation("Successfully uploaded {count} communities to dbo.TotalCommunities", communities.Count);
                        }

                        using (var bulkCopy = new SqlBulkCopy(sqlConnection, bulkCopyOptions, transaction))
                        {
                            bulkCopy.DestinationTableName = "dbo.CommunityOwners";
                            bulkCopy.BatchSize = 50000;
                            bulkCopy.BulkCopyTimeout = 0;

                            bulkCopy.ColumnMappings.Add("Id", "UserId");
                            bulkCopy.ColumnMappings.Add("CommunityId", "CommunityId");
                            bulkCopy.ColumnMappings.Add("SnapshotDate", "SnapshotDate");

                            await bulkCopy.WriteToServerAsync(ownerDataTable);
                            _logger.LogInformation("Successfully uploaded {count} owners to dbo.CommunityOwners", ownerDataTable.Rows.Count);
                        }

                        using (var bulkCopy = new SqlBulkCopy(sqlConnection, bulkCopyOptions, transaction))
                        {
                            bulkCopy.DestinationTableName = "dbo.CommunityMembers";
                            bulkCopy.BatchSize = 50000;
                            bulkCopy.BulkCopyTimeout = 0;

                            bulkCopy.ColumnMappings.Add("Id", "UserId");
                            bulkCopy.ColumnMappings.Add("CommunityId", "CommunityId");
                            bulkCopy.ColumnMappings.Add("SnapshotDate", "SnapshotDate");

                            await bulkCopy.WriteToServerAsync(memberDataTable);
                            _logger.LogInformation("Successfully uploaded {count} members to dbo.CommunityMembers", memberDataTable.Rows.Count);
                        }

                        transaction.Commit();
                    }
                    catch
                    {
                        // If anything fails we rollback the entire transaction
                        transaction.Rollback();
                        throw;
                    }
                }
                else
                {
                    throw new DataException("No communities to upload");
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex.Message);
                throw;
            }

            _logger.LogInformation("Finished processing {blobName}", blobName);
        }
    }
}