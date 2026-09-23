using Microsoft.AspNetCore.Http;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Graph.Models;
using Microsoft.Graph.Search.Query;
using Parquet;
using Parquet.Schema;

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
    public async Task<string> Run([TimerTrigger(Globals.TimerStartTime)] TimerInfo timer)
    //public async Task<string> Run([HttpTrigger(AuthorizationLevel.Function, "get", "post")] HttpRequest req)
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

            var idField = new DataField<string>("Id");
            var siteIdField = new DataField<string>("SiteId");
            var webIdField = new DataField<string>("WebId");
            var listIdField = new DataField<string>("ListId");
            var listItemIdField = new DataField<int>("ListItemId");

            var pathField = new DataField<string>("Path");
            var fileNameField = new DataField<string>("FileName");
            var fileExtensionField = new DataField<string>("FileExtension");
            var titleField = new DataField<string>("Title");
            var spWebUrlField = new DataField<string>("SPWebUrl");
            var spSiteUrlField = new DataField<string>("SPSiteUrl");
            var siteTitleField = new DataField<string>("SiteTitle");

            var createdField = new DataField<DateTime>("Created");
            var lastModifiedTimeField = new DataField<DateTime>("LastModifiedTime");
            var authorField = new DataField<string>("Author");
            var editorField = new DataField<string>("EditorOWSUSER");

            var viewsLifeTimeField = new DataField<int>("ViewsLifeTime");
            var viewsLifeTimeUniqueField = new DataField<int>("ViewsLifeTimeUniqueUsers");

            var viewsLast1DaysField = new DataField<int>("ViewsLast1Days");
            var viewsLast2DaysField = new DataField<int>("ViewsLast2Days");
            var viewsLast3DaysField = new DataField<int>("ViewsLast3Days");
            var viewsLast4DaysField = new DataField<int>("ViewsLast4Days");
            var viewsLast5DaysField = new DataField<int>("ViewsLast5Days");
            var viewsLast6DaysField = new DataField<int>("ViewsLast6Days");
            var viewsLast7DaysField = new DataField<int>("ViewsLast7Days");

            var viewsLastMonths1Field = new DataField<int>("ViewsLastMonths1");
            var viewsLastMonths2Field = new DataField<int>("ViewsLastMonths2");
            var viewsLastMonths3Field = new DataField<int>("ViewsLastMonths3");

            var snapshotDateField = new DataField<DateTime>("SnapshotDate");

            var schema = new ParquetSchema(idField, siteIdField, webIdField, listIdField, listItemIdField, pathField, fileNameField, fileExtensionField, titleField, 
                spWebUrlField, spSiteUrlField, siteTitleField, createdField, lastModifiedTimeField, authorField, editorField, viewsLifeTimeField, 
                viewsLifeTimeUniqueField, viewsLast1DaysField, viewsLast2DaysField, viewsLast3DaysField, viewsLast4DaysField, viewsLast5DaysField, 
                viewsLast6DaysField,viewsLast7DaysField, viewsLastMonths1Field, viewsLastMonths2Field, viewsLastMonths3Field, snapshotDateField);

            await using var parquetWriter = await ParquetWriter.CreateAsync(schema, blobStream, Globals.ParquetOptions);

            var idBuffer = new List<string>(Globals.RowGroupBatchSize);
            var siteIdBuffer = new List<string>(Globals.RowGroupBatchSize);
            var webIdBuffer = new List<string>(Globals.RowGroupBatchSize);
            var listIdBuffer = new List<string>(Globals.RowGroupBatchSize);
            var listItemIdBuffer = new List<int>(Globals.RowGroupBatchSize);

            var pathBuffer = new List<string>(Globals.RowGroupBatchSize);
            var fileNameBuffer = new List<string>(Globals.RowGroupBatchSize);
            var fileExtensionBuffer = new List<string>(Globals.RowGroupBatchSize);
            var titleBuffer = new List<string>(Globals.RowGroupBatchSize);
            var spWebUrlBuffer = new List<string>(Globals.RowGroupBatchSize);
            var spSiteUrlBuffer = new List<string>(Globals.RowGroupBatchSize);
            var siteTitleBuffer = new List<string>(Globals.RowGroupBatchSize);

            var createdBuffer = new List<DateTime>(Globals.RowGroupBatchSize);
            var lastModifiedTimeBuffer = new List<DateTime>(Globals.RowGroupBatchSize);
            var authorBuffer = new List<string>(Globals.RowGroupBatchSize);
            var editorBuffer = new List<string>(Globals.RowGroupBatchSize);

            var viewsLifeTimeBuffer = new List<int>(Globals.RowGroupBatchSize);
            var viewsLifeTimeUniqueUsersBuffer = new List<int>(Globals.RowGroupBatchSize);

            var viewsLast1DaysBuffer = new List<int>(Globals.RowGroupBatchSize);
            var viewsLast2DaysBuffer = new List<int>(Globals.RowGroupBatchSize);
            var viewsLast3DaysBuffer = new List<int>(Globals.RowGroupBatchSize);
            var viewsLast4DaysBuffer = new List<int>(Globals.RowGroupBatchSize);
            var viewsLast5DaysBuffer = new List<int>(Globals.RowGroupBatchSize);
            var viewsLast6DaysBuffer = new List<int>(Globals.RowGroupBatchSize);
            var viewsLast7DaysBuffer = new List<int>(Globals.RowGroupBatchSize);

            var ViewsLastMonths1Buffer = new List<int>(Globals.RowGroupBatchSize);
            var ViewsLastMonths2Buffer = new List<int>(Globals.RowGroupBatchSize);
            var ViewsLastMonths3Buffer = new List<int>(Globals.RowGroupBatchSize);

            var snapshotDateBuffer = new List<DateTime>(Globals.RowGroupBatchSize);

            async Task FlushBatchAsync()
            {
                if (idBuffer.Count == 0)
                    return;

                using var groupWriter = parquetWriter.CreateRowGroup();

                await groupWriter.WriteAsync(idField, idBuffer);
                await groupWriter.WriteAsync(siteIdField, siteIdBuffer);
                await groupWriter.WriteAsync(webIdField, webIdBuffer);
                await groupWriter.WriteAsync(listIdField, listIdBuffer);
                await groupWriter.WriteAsync<int>(listItemIdField, listItemIdBuffer.ToArray().AsMemory());

                await groupWriter.WriteAsync(pathField, pathBuffer);
                await groupWriter.WriteAsync(fileNameField, fileNameBuffer);
                await groupWriter.WriteAsync(fileExtensionField, fileExtensionBuffer);
                await groupWriter.WriteAsync(titleField, titleBuffer);
                await groupWriter.WriteAsync(spWebUrlField, spWebUrlBuffer);
                await groupWriter.WriteAsync(spSiteUrlField, spSiteUrlBuffer);
                await groupWriter.WriteAsync(siteTitleField, siteTitleBuffer);

                await groupWriter.WriteAsync<DateTime>(createdField, createdBuffer.ToArray().AsMemory());
                await groupWriter.WriteAsync<DateTime>(lastModifiedTimeField, lastModifiedTimeBuffer.ToArray().AsMemory());
                await groupWriter.WriteAsync(authorField, authorBuffer);
                await groupWriter.WriteAsync(editorField, editorBuffer);

                await groupWriter.WriteAsync<int>(viewsLifeTimeField, viewsLifeTimeBuffer.ToArray().AsMemory());
                await groupWriter.WriteAsync<int>(viewsLifeTimeUniqueField, viewsLifeTimeUniqueUsersBuffer.ToArray().AsMemory());

                await groupWriter.WriteAsync<int>(viewsLast1DaysField, viewsLast1DaysBuffer.ToArray().AsMemory());
                await groupWriter.WriteAsync<int>(viewsLast2DaysField, viewsLast2DaysBuffer.ToArray().AsMemory());
                await groupWriter.WriteAsync<int>(viewsLast3DaysField, viewsLast3DaysBuffer.ToArray().AsMemory());
                await groupWriter.WriteAsync<int>(viewsLast4DaysField, viewsLast4DaysBuffer.ToArray().AsMemory());
                await groupWriter.WriteAsync<int>(viewsLast5DaysField, viewsLast5DaysBuffer.ToArray().AsMemory());
                await groupWriter.WriteAsync<int>(viewsLast6DaysField, viewsLast6DaysBuffer.ToArray().AsMemory());
                await groupWriter.WriteAsync<int>(viewsLast7DaysField, viewsLast7DaysBuffer.ToArray().AsMemory());

                await groupWriter.WriteAsync<int>(viewsLastMonths1Field, ViewsLastMonths1Buffer.ToArray().AsMemory());
                await groupWriter.WriteAsync<int>(viewsLastMonths2Field, ViewsLastMonths2Buffer.ToArray().AsMemory());
                await groupWriter.WriteAsync<int>(viewsLastMonths3Field, ViewsLastMonths3Buffer.ToArray().AsMemory());

                await groupWriter.WriteAsync<DateTime>(snapshotDateField, snapshotDateBuffer.ToArray().AsMemory());

                idBuffer.Clear();
                siteIdBuffer.Clear();
                webIdBuffer.Clear();
                listIdBuffer.Clear();
                listItemIdBuffer.Clear();

                pathBuffer.Clear();
                fileNameBuffer.Clear();
                titleBuffer.Clear();
                spWebUrlBuffer.Clear();
                spSiteUrlBuffer.Clear();
                siteTitleBuffer.Clear();

                createdBuffer.Clear();
                lastModifiedTimeBuffer.Clear();
                authorBuffer.Clear();
                editorBuffer.Clear();

                viewsLifeTimeBuffer.Clear();
                viewsLifeTimeUniqueUsersBuffer.Clear();

                viewsLast1DaysBuffer.Clear();
                viewsLast2DaysBuffer.Clear();
                viewsLast3DaysBuffer.Clear();
                viewsLast4DaysBuffer.Clear();
                viewsLast5DaysBuffer.Clear();
                viewsLast6DaysBuffer.Clear();
                viewsLast7DaysBuffer.Clear();

                ViewsLastMonths1Buffer.Clear();
                ViewsLastMonths2Buffer.Clear();
                ViewsLastMonths3Buffer.Clear();

                snapshotDateBuffer.Clear();
            }

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
                                        "Created",
                                        "LastModifiedTime",
                                        "Author",
                                        "EditorOWSUSER",
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

                            idBuffer.Add(Globals.GetField(fields, "UniqueId").Trim('{', '}'));
                            siteIdBuffer.Add(Globals.GetField(fields, "SiteId"));
                            webIdBuffer.Add(Globals.GetField(fields, "WebId"));
                            listIdBuffer.Add(Globals.GetField(fields, "ListId"));
                            listItemIdBuffer.Add(int.Parse(Globals.GetField(fields, "ListItemId")));

                            pathBuffer.Add(Globals.GetField(fields, "Path"));
                            fileNameBuffer.Add(Globals.GetField(fields, "FileName"));
                            fileExtensionBuffer.Add(Globals.GetField(fields, "FileExtension"));
                            titleBuffer.Add(Globals.GetField(fields, "Title"));
                            spWebUrlBuffer.Add(Globals.GetField(fields, "SPWebUrl"));
                            spSiteUrlBuffer.Add(Globals.GetField(fields, "SPSiteUrl"));
                            siteTitleBuffer.Add(Globals.GetField(fields, "SiteTitle"));

                            createdBuffer.Add(DateTime.Parse(Globals.GetField(fields, "Created")));
                            lastModifiedTimeBuffer.Add(DateTime.Parse(Globals.GetField(fields, "LastModifiedTime")));
                            authorBuffer.Add(Globals.GetField(fields, "Author"));
                            editorBuffer.Add(Globals.GetField(fields, "EditorOWSUSER"));

                            viewsLifeTimeBuffer.Add(int.Parse(Globals.GetField(fields, "ViewsLifeTime") ?? "0"));
                            viewsLifeTimeUniqueUsersBuffer.Add(int.Parse(Globals.GetField(fields, "ViewsLifeTimeUniqueUsers") ?? "0"));

                            viewsLast1DaysBuffer.Add(int.Parse(Globals.GetField(fields, "ViewsLast1Days") ?? "0"));
                            viewsLast2DaysBuffer.Add(int.Parse(Globals.GetField(fields, "ViewsLast2Days") ?? "0"));
                            viewsLast3DaysBuffer.Add(int.Parse(Globals.GetField(fields, "ViewsLast3Days") ?? "0"));
                            viewsLast4DaysBuffer.Add(int.Parse(Globals.GetField(fields, "ViewsLast4Days") ?? "0"));
                            viewsLast5DaysBuffer.Add(int.Parse(Globals.GetField(fields, "ViewsLast5Days") ?? "0"));
                            viewsLast6DaysBuffer.Add(int.Parse(Globals.GetField(fields, "ViewsLast6Days") ?? "0"));
                            viewsLast7DaysBuffer.Add(int.Parse(Globals.GetField(fields, "ViewsLast7Days") ?? "0"));

                            ViewsLastMonths1Buffer.Add(int.Parse(Globals.GetField(fields, "ViewsLastMonths1") ?? "0"));
                            ViewsLastMonths2Buffer.Add(int.Parse(Globals.GetField(fields, "ViewsLastMonths2") ?? "0"));
                            ViewsLastMonths3Buffer.Add(int.Parse(Globals.GetField(fields, "ViewsLastMonths3") ?? "0"));

                            snapshotDateBuffer.Add(snapshotDate);

                            if (idBuffer.Count >= Globals.RowGroupBatchSize)
                            {
                                FlushBatchAsync().GetAwaiter().GetResult();
                            }
                        }
                    }

                    if (hits.Count == 0) break;
                    from += hits.Count;
                }
                while (moreResultsAvailable && from < total);

                _logger.LogInformation($"Found {total} files for {monthStart:yyyy-MM}");
            }

            _logger.LogInformation($"Processed {runningTotal} total files");

            await FlushBatchAsync();
            await parquetWriter.DisposeAsync();

            return blobName;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex.Message);
            throw;
        }
    }
}