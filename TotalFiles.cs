using CommunityToolkit.HighPerformance.Buffers;
using Microsoft.AspNetCore.Http;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Graph.Models;
using Microsoft.Graph.Search.Query;
using Microsoft.Kiota.Abstractions.Serialization;
using Parquet;
using Parquet.Schema;
using System.Text.Json;

namespace GCStats;

public class TotalFiles
{
    private readonly ILogger<TotalFiles> _logger;
    private readonly IConfiguration _config;

    public const string FilesContainerName = "files";

    public TotalFiles(ILogger<TotalFiles> logger, IConfiguration config)
    {
        _logger = logger;
        _config = config;
    }

    [Function("TotalFiles")]
    [QueueOutput("process-total-files", Connection = "AzureWebJobsStorage")]
    //public async Task<string> Run([TimerTrigger(Globals.TimerStartTime)] TimerInfo timer)
    public async Task<string> Run([HttpTrigger(AuthorizationLevel.Function, "get", "post")] HttpRequest req)
    {
        _logger.LogInformation("TotalFiles timer trigger executed at: {Time}", DateTime.UtcNow);

        var blobName = await StreamTotalFilesToBlobAsync();

        return blobName;
    }

    public async Task<string> StreamTotalFilesToBlobAsync()
    {
        try
        {
            var snapshotDate = DateTime.UtcNow.Date;
            var blobName = $"{FilesContainerName}-{DateTime.UtcNow.ToString(Globals.BlobDateFormat)}.parquet";

            var graphClient = Auth.GetGraphServiceClient(_logger);
            var blobClient = await Auth.GetBlobClient(FilesContainerName, blobName, _logger, _config);

            using var blobStream = await blobClient.OpenWriteAsync(overwrite: true);

            const int PageSize = 500;
            long runningTotal = 0;

            var endDate = DateTime.UtcNow;
            var startDate = new DateTime(2021, 1, 1); // Starts in 2021 when GCX was officially released

            var monthRanges = new List<(DateTime Start, DateTime End)>();
            var cursor = new DateTime(startDate.Year, startDate.Month, 1);

            while (cursor < endDate)
            {
                var monthStart = cursor;
                var monthEnd = cursor.AddMonths(1).AddSeconds(-1);
                monthRanges.Add((monthStart, monthEnd));
                cursor = cursor.AddMonths(1);
            }

            foreach (var (monthStart, monthEnd) in monthRanges)
            {
                int from = 0;
                long total = 0;
                bool moreResultsAvailable;

                var startCreation = monthStart.ToString("yyyy-MM-ddTHH:mm:ss");
                var endCreation = monthEnd.ToString("yyyy-MM-ddTHH:mm:ss");

                _logger.LogInformation($"Searching: {monthStart:yyyy-MM}");

                do
                {
                    var searchResponse = await graphClient.Search.Query.PostAsQueryPostResponseAsync(
                        new QueryPostRequestBody
                        {
                            Requests = new List<SearchRequest>
                            {
                                new SearchRequest
                                {
                                    EntityTypes = new List<EntityType?> { EntityType.ListItem },
                                    Region = "CAN",
                                    Query = new SearchQuery
                                    {
                                        QueryString = $"isDocument=true AND Created:{startCreation}..{endCreation} AND NOT FileExtension:aspx AND NOT Author:\"System Account\""
                                    },
                                    Fields = new List<string>
                                    {
                                        "UniqueId",
                                        "SiteId",
                                        "WebId",
                                        "ListId",
                                        "ListItemId",
                                        "Path",
                                        "FileName",
                                        "FileExtension",
                                        "Title",
                                        "SPWebUrl",
                                        "SPSiteUrl",
                                        "SiteTitle",
                                        "PromotedState",
                                        "Created",
                                        "LastModifiedTime",
                                        "Author",
                                        "EditorOWSUSER",
                                        "SPTranslationLanguage",
                                        "ViewsLifeTime",
                                        "ViewsLifeTimeUniqueUsers",
                                        "ViewsLast1Days",
                                        "ViewsLast2Days",
                                        "ViewsLast3Days",
                                        "ViewsLast4Days",
                                        "ViewsLast5Days",
                                        "ViewsLast6Days",
                                        "ViewsLast7Days",
                                        "ViewsLastMonths1",
                                        "ViewsLastMonths2",
                                        "ViewsLastMonths3",
                                    },
                                    From = from,
                                    Size = PageSize,
                                    SortProperties = new List<SortProperty>
                                    {
                                        new SortProperty
                                        {
                                            Name = "ViewsLifeTime",
                                            IsDescending = true
                                        }
                                    }
                                }
                            }
                        }
                    );

                    var container = searchResponse?.Value?.FirstOrDefault()?.HitsContainers?.FirstOrDefault();

                    var hits = container?.Hits ?? new List<SearchHit>();
                    runningTotal += hits.Count;
                    total = container?.Total ?? 0;
                    moreResultsAvailable = container?.MoreResultsAvailable ?? false;

                    foreach (var hit in hits)
                    {
                        var listItem = hit.Resource as ListItem;

                        if (listItem != null && listItem.Fields != null && listItem.Fields.AdditionalData != null)
                        {
                            var fields = listItem.Fields.AdditionalData;

                            foreach (var field in fields)
                                _logger.LogInformation($"{field.Key.ToString()}: {field.Value.ToString()}");
                        }
                    }

                    if (hits.Count == 0) break;
                    from += hits.Count;
                }
                while (moreResultsAvailable && from < total);

                _logger.LogInformation($"Found {total} files for {monthStart:yyyy-MM}");
            }

            return blobName;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex.Message);
            throw;
        }
    }
}