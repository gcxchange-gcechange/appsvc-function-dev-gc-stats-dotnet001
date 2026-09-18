using Azure.Identity;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
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

namespace GCStats
{
    public class PageViews
    {
        private readonly ILogger<PageViews> _logger;
        private readonly IConfiguration _config;

        public const string PageViewsContainerName = "page-views";

        public PageViews(ILogger<PageViews> logger, IConfiguration config)
        {
            _logger = logger;
            _config = config;
        }

        [Function("PageViews")]
        [QueueOutput("process-page-views", Connection = "AzureWebJobsStorage")]
        public async Task<string> Run([HttpTrigger(AuthorizationLevel.Function, "get", "post")] HttpRequest req)
        {
            _logger.LogInformation("PageViews timer trigger executed at: {Time}", DateTime.UtcNow);

            var blobName = await StreamPageViewsToBlobAsync();

            return blobName;
        }

        public async Task<string> StreamPageViewsToBlobAsync()
        {
            try
            {
                var snapshotDate = DateTime.UtcNow.Date;
                var blobName = $"{PageViewsContainerName}-{DateTime.UtcNow.ToString(Globals.BlobDateFormat)}.parquet";

                var graphClient = Auth.GetGraphServiceClient(_logger);
                var blobClient = await Auth.GetBlobClient(PageViewsContainerName, blobName, _logger, _config);

                using var blobStream = await blobClient.OpenWriteAsync(overwrite: true);

                var idField = new DataField<string>("Id");
                var siteIdField = new DataField<string>("SiteId");
                var titleField = new DataField<string>("Title");
                var urlField = new DataField<string>("URL");
                var viewsField = new DataField<string>("Views");
                var viewsPreviousDayField = new DataField<string>("ViewsPreviousDay");
                var languageField = new DataField<string>("Language");
                var snapshotDateField = new DataField<DateTime>("SnapshotDate");

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
                    var endCreation= monthEnd.ToString("yyyy-MM-ddTHH:mm:ss");

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
                                        QueryString = $"FileExtension:aspx AND NOT PromotedState:2 AND NOT Author:\"System Account\" AND Created:{startCreation}..{endCreation}"
                                    },
                                    Fields = new List<string> { "UniqueId", "SiteId", "Title", "Path", "Created", "LastModifiedTime", "ViewsLifeTime", "ViewsLast1Days", "SPTranslationLanguage", "Author", "EditorOWSUSER" },
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
                                var id = GetField(fields, "UniqueId");
                                var siteId = GetField(fields, "SiteId");
                                var title = GetField(fields, "Title");
                                var path = GetField(fields, "Path");
                                var created = GetField(fields, "Created");
                                var lastModifiedTime = GetField(fields, "LastModifiedTime");
                                var views = GetField(fields, "ViewsLifeTime") ?? "0";
                                var viewsPreviousDay = GetField(fields, "ViewsLast1Days") ?? "0";
                                var language = GetField(fields, "SPTranslationLanguage") ?? "en";
                                var author = GetField(fields, "Author");
                                var editor = GetField(fields, "EditorOWSUSER");

                                //_logger.LogInformation($"\nTitle: {title}\nCreated: {created}\nLastModifiedTime: {lastModifiedTime}\nPath: {path}\nViews: {views}\nViewsPreviousDay: {viewsPreviousDay}\nLanguage: {language}\nAuthor: {author}\nEditor: {editor}\n");

                                // TODO: Sort by SiteID and UniqueId before writing parquet
                            }
                        }

                        if (hits.Count == 0) break;
                        from += hits.Count;
                    }
                    while (moreResultsAvailable && from < total /*&& from < 10000*/);

                    _logger.LogInformation($"Found {total} pages for {monthStart:yyyy-MM}");
                }

                _logger.LogInformation($"Processed {runningTotal} total pages");

                return blobName;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex.Message);
                throw;
            }
        }

        private string GetField(IDictionary<string, object> fields, string name)
        {
            if (fields is null) return null;

            // key casing varies, so match case-insensitively
            var key = fields.Keys.FirstOrDefault(k => string.Equals(k, name, StringComparison.OrdinalIgnoreCase));
            if (key is null) return null;

            return fields[key] switch
            {
                UntypedString s => s.GetValue(),
                UntypedInteger i => i.GetValue().ToString(),
                UntypedLong l => l.GetValue().ToString(),
                JsonElement je => je.ToString(),
                var v => v?.ToString()
            };
        }
    }
}