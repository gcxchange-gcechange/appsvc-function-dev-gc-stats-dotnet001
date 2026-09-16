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

        private const string PageViewsContainerName = "page-views";

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

                var blobClient = Auth.GetBlobClient(PageViewsContainerName, blobName, _logger, _config);
                var graphClient = Auth.GraphAuth(_logger);

                var idField = new DataField<string>("Id");
                var siteIdField = new DataField<string>("SiteId");
                var titleField = new DataField<string>("Title");
                var viewsLifeTimeField = new DataField<string>("ViewsLifeTime");
                var viewsPreviousDayField = new DataField<string>("ViewsPreviousDay");
                var languageField = new DataField<string>("Language");
                var snapshotDateField = new DataField<DateTime>("SnapshotDate");

                var parquetOptions = new ParquetOptions
                {
                    CompressionMethod = CompressionMethod.Zstd
                };

                const int PageSize = 10;
                int from = 0;
                long total = 0;
                bool moreResultsAvailable;

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
                                        QueryString = "FileExtension:aspx AND NOT PromotedState:2 AND NOT Title:DispForm.aspx" // Excludes news posts
                                    },
                                    Fields = new List<string> { "UniqueId", "SiteId", "Title", "LastModifiedTime", "ViewsLifeTime", "ViewsLifeTimeUniqueUsers", "ViewsLAst1Days", "SPTranslationLanguage" },
                                    From = from,
                                    Size = PageSize,
                                    SortProperties = new List<SortProperty>
                                    {
                                        new SortProperty { 
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
                    total = container?.Total ?? 0;
                    moreResultsAvailable = container?.MoreResultsAvailable ?? false;

                    foreach (var hit in hits)
                    {
                        var listItem = hit.Resource as ListItem;

                        if (listItem != null && listItem.Fields != null && listItem.Fields.AdditionalData != null)
                        {
                            var fields = listItem.Fields.AdditionalData;
                            var id = GetField(fields, "uniqueId");
                            var siteId = GetField(fields, "siteId");
                            var title = GetField(fields, "title");
                            var views = GetField(fields, "viewsLifeTime") ?? "0";
                            var viewsPreviousDay = GetField(fields, "viewsLast1Days") ?? "0";
                            var language = GetField(fields, "SPTranslationLanguage");

                            // TODO: Sort by SiteID and UniqueId before writing parquet

                            _logger.LogInformation("Title: {Title}, Views: {Views}, ViewsPreviousDay: {ViewsPreviousDay}, Language: {Language}", title, views, viewsPreviousDay, language);
                        }
                    }

                    if (hits.Count == 0) break;
                    from += hits.Count;
                }
                while (moreResultsAvailable && from < total);

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