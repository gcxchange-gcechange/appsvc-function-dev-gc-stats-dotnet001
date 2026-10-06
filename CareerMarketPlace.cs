using Google.Protobuf.Collections;
using Microsoft.AspNetCore.Http;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Graph;
using Microsoft.Graph.Models;
using Parquet;
using Parquet.Schema;

namespace GCStats
{
    public class JobOpportunities
    {
        private readonly ILogger<JobOpportunities> _logger;
        private readonly IConfiguration _config;

        public const string JobOpportunitiesContainerName = "job-opportunities";

        public JobOpportunities(ILogger<JobOpportunities> logger, IConfiguration config)
        {
            _logger = logger;
            _config = config;
        }

        [Function("JobOpportunities")]
        [QueueOutput("process-job-opportunities", Connection = "AzureWebJobsStorage")]
        public async Task<string> Run([HttpTrigger(AuthorizationLevel.Function, "get", "post")] HttpRequest req)
        //public async Task<string> Run([TimerTrigger(Globals.TimerStartTime)] TimerInfo timer)
        {
            _logger.LogInformation("JobOpportunities timer trigger executed at: {Time}", DateTime.UtcNow);

            var blobName = await StreamJobOpportunitiesToBlobAsync();

            return blobName;
        }

        public async Task<string> StreamJobOpportunitiesToBlobAsync()
        {
            try
            {
                var snapshotDate = DateTime.UtcNow.Date;
                var blobName = $"{JobOpportunitiesContainerName}-{DateTime.UtcNow.ToString(Globals.BlobDateFormat)}.parquet";

                var graphClient = Auth.GetGraphServiceClient(_logger);
                var blobClient = await Auth.GetBlobClient(JobOpportunitiesContainerName, blobName, _logger, _config);

                using var blobStream = await blobClient.OpenWriteAsync(overwrite: true);

                // TODO: 1. Capture total live job postings daily (save all columns)
                //       2. Capture views for each post daily
                //       3. Capture number of apply clicks total for all jobs
                //       4. Capture number of apply clicks per job
                //       5. Capture top 100 skills daily
                //       6. Capture average lifespan of opportunity

                // Parquet files
                //       1. JobOpportunities (1, 2, 3 + 4, 5, 6)

                //       2. JobOpportunitySkills
                //          Id
                //          JobOpportunityId
                //          SkillId

                //       3. JobOpportunityJobTypes
                //          Id
                //          JobOpportunityId
                //          JobTypeId

                var idField = new DataField<string>("Id");
                var authorMailField = new DataField<string>("AuthorMail");
                var departmentIdField = new DataField<string>("DepartmentId");
                var titleEnField = new DataField<string>("JobTitleEn");
                var titleFrField = new DataField<string>("JobTitleFr");
                var classCodeIdField = new DataField<string>("ClassificationCodeId");
                var classLevelIdField = new DataField<string>("ClassificationLevelId");
                var numOpportunitiesField = new DataField<int>("NumberOfOpportunities");
                var durationIdField = new DataField<string>("DurationId");
                var durationQuantityField = new DataField<int>("DurationQuantity");
                var creationDateField = new DataField<DateTime>("CreationDate");
                var modificationDateField = new DataField<DateTime>("ModificationDate");
                var applicationDeadlineDateField = new DataField<DateTime>("ApplicationDeadlineDate");
                var jobDescriptionEnField = new DataField<string>("JobDescriptionEn");
                var jobDescriptionFrField = new DataField<string>("JobDescriptionFr");
                var workscheduleIdField = new DataField<string>("WorkScheduleId");
                var securityClearanceIdField = new DataField<string>("SecurityClearanceId");
                var languageComprehensionField = new DataField<string>("LanguageComprehension");
                var languageRequirementIdField = new DataField<string>("LanguageRequirementId");
                var workArrangementIdField = new DataField<string>("WorkArrangementId");
                var approvedStaffindField = new DataField<bool>("ApprovedStaffing");
                var cityIdField = new DataField<string>("CityId");
                var programAreaIdField = new DataField<string>("ProgramAreaId");
                var snapshotDateField = new DataField<DateTime>("SnapshotDate");

                var schema = new ParquetSchema(idField, authorMailField, departmentIdField, titleEnField, titleFrField, classCodeIdField, classLevelIdField, numOpportunitiesField,
                    durationIdField, durationQuantityField, creationDateField, modificationDateField, applicationDeadlineDateField, jobDescriptionEnField, jobDescriptionFrField,
                    workscheduleIdField, securityClearanceIdField, languageComprehensionField, languageRequirementIdField, workArrangementIdField, approvedStaffindField,
                    cityIdField, programAreaIdField, snapshotDateField);

                await using var parquetWriter = await ParquetWriter.CreateAsync(schema, blobStream, Globals.ParquetOptions);

                var idBuffer = new List<string>(Globals.RowGroupBatchSize);
                var authorMailBuffer = new List<string>(Globals.RowGroupBatchSize);
                var departmentIdBuffer = new List<string>(Globals.RowGroupBatchSize);
                var titleEnBuffer = new List<string>(Globals.RowGroupBatchSize);
                var titleFrBuffer = new List<string>(Globals.RowGroupBatchSize);
                var classCodeIdBuffer = new List<string>(Globals.RowGroupBatchSize);
                var classLevelIdBuffer = new List<string>(Globals.RowGroupBatchSize);
                var numOpportunitiesBuffer = new List<int>(Globals.RowGroupBatchSize);
                var durationIdBuffer = new List<string>(Globals.RowGroupBatchSize);
                var durationQuantityBuffer = new List<int>(Globals.RowGroupBatchSize);
                var creationDateBuffer = new List<DateTime>(Globals.RowGroupBatchSize);
                var modificationDateBuffer = new List<DateTime>(Globals.RowGroupBatchSize);
                var applicationDeadlineDateBuffer = new List<DateTime>(Globals.RowGroupBatchSize);
                var jobDescriptionEnBuffer = new List<string>(Globals.RowGroupBatchSize);
                var jobDescriptionFrBuffer = new List<string>(Globals.RowGroupBatchSize);
                var workscheduleIdBuffer = new List<string>(Globals.RowGroupBatchSize);
                var securityClearanceIdBuffer = new List<string>(Globals.RowGroupBatchSize);
                var languageComprehensionBuffer = new List<string>(Globals.RowGroupBatchSize);
                var languageRequirementIdBuffer = new List<string>(Globals.RowGroupBatchSize);
                var workArrangementIdBuffer = new List<string>(Globals.RowGroupBatchSize);
                var approvedStaffindBuffer = new List<bool>(Globals.RowGroupBatchSize);
                var cityIdBuffer = new List<string>(Globals.RowGroupBatchSize);
                var programAreaIdBuffer = new List<string>(Globals.RowGroupBatchSize);
                var snapshotDateBuffer = new List<DateTime>(Globals.RowGroupBatchSize);

                async Task FlushBatchAsync()
                {
                    if (idBuffer.Count == 0)
                        return;

                    using var groupWriter = parquetWriter.CreateRowGroup();

                    await groupWriter.WriteAsync(idField, idBuffer);
                    await groupWriter.WriteAsync(authorMailField, authorMailBuffer);
                    await groupWriter.WriteAsync(departmentIdField, departmentIdBuffer);
                    await groupWriter.WriteAsync(titleEnField, titleEnBuffer);
                    await groupWriter.WriteAsync(titleFrField, titleFrBuffer);
                    await groupWriter.WriteAsync(classCodeIdField, classCodeIdBuffer);
                    await groupWriter.WriteAsync(classLevelIdField, classLevelIdBuffer);
                    await groupWriter.WriteAsync<int>(numOpportunitiesField, numOpportunitiesBuffer.ToArray().AsMemory());
                    await groupWriter.WriteAsync(durationIdField, durationIdBuffer);
                    await groupWriter.WriteAsync<int>(durationQuantityField, durationQuantityBuffer.ToArray().AsMemory());
                    await groupWriter.WriteAsync<DateTime>(creationDateField, creationDateBuffer.ToArray().AsMemory());
                    await groupWriter.WriteAsync<DateTime>(modificationDateField, modificationDateBuffer.ToArray().AsMemory());
                    await groupWriter.WriteAsync<DateTime>(applicationDeadlineDateField, applicationDeadlineDateBuffer.ToArray().AsMemory());
                    await groupWriter.WriteAsync(jobDescriptionEnField, jobDescriptionEnBuffer);
                    await groupWriter.WriteAsync(jobDescriptionFrField, jobDescriptionFrBuffer);
                    await groupWriter.WriteAsync(workscheduleIdField, workscheduleIdBuffer);
                    await groupWriter.WriteAsync(securityClearanceIdField, securityClearanceIdBuffer);
                    await groupWriter.WriteAsync(languageComprehensionField, languageComprehensionBuffer);
                    await groupWriter.WriteAsync(languageRequirementIdField, languageRequirementIdBuffer);
                    await groupWriter.WriteAsync(workArrangementIdField, workArrangementIdBuffer);
                    await groupWriter.WriteAsync<bool>(approvedStaffindField, approvedStaffindBuffer.ToArray().AsMemory());
                    await groupWriter.WriteAsync(cityIdField, cityIdBuffer);
                    await groupWriter.WriteAsync(programAreaIdField, programAreaIdBuffer);
                    await groupWriter.WriteAsync<DateTime>(snapshotDateField, snapshotDateBuffer.ToArray().AsMemory());

                    idBuffer.Clear();
                    authorMailBuffer.Clear();
                    departmentIdBuffer.Clear();
                    titleEnBuffer.Clear();
                    titleFrBuffer.Clear();
                    classCodeIdBuffer.Clear();
                    classLevelIdBuffer.Clear();
                    numOpportunitiesBuffer.Clear();
                    durationIdBuffer.Clear();
                    durationQuantityBuffer.Clear();
                    creationDateBuffer.Clear();
                    modificationDateBuffer.Clear();
                    applicationDeadlineDateBuffer.Clear();
                    jobDescriptionEnBuffer.Clear();
                    jobDescriptionFrBuffer.Clear();
                    workscheduleIdBuffer.Clear();
                    securityClearanceIdBuffer.Clear();
                    languageComprehensionBuffer.Clear();
                    languageRequirementIdBuffer.Clear();
                    workArrangementIdBuffer.Clear();
                    approvedStaffindBuffer.Clear();
                    cityIdBuffer.Clear();
                    programAreaIdBuffer.Clear();
                    snapshotDateBuffer.Clear();
                }

                var siteId = Auth.GetAppSetting("cmSiteId", _logger, _config);
                var listId = Auth.GetAppSetting("cmListId", _logger, _config);

                var jobOpportunityPage = await graphClient.Sites[siteId].Lists[listId].Items.GetAsync(rc =>
                {
                    rc.QueryParameters.Expand = new[] { "fields" };
                });

                //var cols = await graphClient.Sites[siteId].Lists[listId].Columns.GetAsync();

                //var lookups = cols!.Value!
                //    .Where(c => c.Lookup != null && c.Hidden != true)
                //    .Select(c => new { c.Name, c.Lookup!.ListId, c.Lookup.ColumnName });

                //foreach (var l in lookups)
                //    Console.WriteLine($"{l.Name} -> list {l.ListId}, column {l.ColumnName}");


                var pageIterator = PageIterator<ListItem, ListItemCollectionResponse>
                    .CreatePageIterator(
                        graphClient,
                        jobOpportunityPage!,
                        jobOpportunity =>
                        {
                            var fields = jobOpportunity.Fields?.AdditionalData;
                            if (jobOpportunity.Id != null && fields != null)
                            {
                                idBuffer.Add(jobOpportunity.Id);
                                authorMailBuffer.Add((string)fields["ContactEmail"]);
                                departmentIdBuffer.Add((string)fields["DepartmentLookupId"]);
                                titleEnBuffer.Add((string)fields["JobTitleEn"]);
                                titleFrBuffer.Add((string)fields["JobTitleFr"]);
                                classCodeIdBuffer.Add((string)fields["ClassificationCodeLookupId"]);
                                classLevelIdBuffer.Add((string)fields["ClassificationLevelLookupId"]);
                                numOpportunitiesBuffer.Add((int)fields["NumberOfOpportunities"]);
                                durationIdBuffer.Add((string)fields["DurationId"]);
                                durationQuantityBuffer.Add((int)fields["DurationQuantity"]);
                                creationDateBuffer.Add((DateTime)fields["Created"]);
                                modificationDateBuffer.Add((DateTime)fields["Modified"]);
                                applicationDeadlineDateBuffer.Add((DateTime)fields["ApplicationDeadlineDate"]);
                                jobDescriptionEnBuffer.Add((string)fields["JobDescriptionEn"]);
                                jobDescriptionFrBuffer.Add((string)fields["JobDescriptionFr"]);
                                workscheduleIdBuffer.Add((string)fields["WorkScheduleId"]);
                                securityClearanceIdBuffer.Add((string)fields["SecurityClearanceId"]);
                                languageComprehensionBuffer.Add((string)fields["LanguageComprehension"]);
                                languageRequirementIdBuffer.Add((string)fields["LanguageRequirementId"]);
                                workArrangementIdBuffer.Add((string)fields["WorkArrangementId"]);
                                approvedStaffindBuffer.Add((bool)fields["ApprovedStaffing"]);
                                cityIdBuffer.Add((string)fields["CityId"]);
                                programAreaIdBuffer.Add((string)fields["ProgramAreaId"]);
                                snapshotDateBuffer.Add(snapshotDate);

                                if (idBuffer.Count >= Globals.RowGroupBatchSize)
                                {
                                    FlushBatchAsync().GetAwaiter().GetResult();
                                }
                            }
                            return true;
                        });

                await pageIterator.IterateAsync();

                return blobName;
            }
            catch (Exception ex)
            {
                _logger.LogError("StreamJobOpportunitiesToBlobAsync failed");
                _logger.LogError(ex.Message);
                throw;
            }
        }
    }
}
