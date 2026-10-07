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
        public const string JobOpportunitySkillsContainerName = "job-opportunity-skills";
        public const string JobOpportunityJobTypesContainerName = "job-opportunity-job-types";

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

                var blobNameJobOpportunities = $"{JobOpportunitiesContainerName}-{DateTime.UtcNow.ToString(Globals.BlobDateFormat)}.parquet";
                var blobNameSkills = $"{JobOpportunitySkillsContainerName}-{DateTime.UtcNow.ToString(Globals.BlobDateFormat)}.parquet";
                var blobNameJobTypes = $"{JobOpportunityJobTypesContainerName}-{DateTime.UtcNow.ToString(Globals.BlobDateFormat)}.parquet";

                var graphClient = Auth.GetGraphServiceClient(_logger);
                var blobClientJobOpp = await Auth.GetBlobClient(JobOpportunitiesContainerName, blobNameJobOpportunities, _logger, _config);

                using var blobStreamJobOpp = await blobClientJobOpp.OpenWriteAsync(overwrite: true);

                // TODO: 1. Capture total live job postings daily (save all columns)
                //       2. Capture views for each post daily
                //       3. Capture number of apply clicks total for all jobs
                //       4. Capture number of apply clicks per job
                //       5. Capture top 100 skills daily
                //       6. Capture average lifespan of opportunity

                // Parquet files
                //       1. JobOpportunities (1, 2, 3 + 4, 5, 6)

                //       2. JobOpportunitySkills
                //          JobOpportunityId
                //          SkillId
                //          Title

                //       3. JobOpportunityJobTypes
                //          JobOpportunityId
                //          JobTypeId
                //          Title

                // FIELDS
                var idField = new DataField<string>("Id");
                var authorMailField = new DataField<string>("AuthorMail");
                var departmentIdField = new DataField<string>("DepartmentId");
                var departmentField = new DataField<string>("Department");
                var titleEnField = new DataField<string>("JobTitleEn");
                var titleFrField = new DataField<string>("JobTitleFr");
                var classCodeIdField = new DataField<string>("ClassificationCodeId");
                var classCodeField = new DataField<string>("ClassificationCode");
                var classLevelIdField = new DataField<string>("ClassificationLevelId");
                var classLevelField = new DataField<string>("ClassificationLevel");
                var numOpportunitiesField = new DataField<int>("NumberOfOpportunities");
                var durationIdField = new DataField<string?>("DurationId");
                var durationField = new DataField<string?>("Duration");
                var durationQuantityField = new DataField<double>("DurationQuantity");
                var creationDateField = new DataField<DateTime>("CreationDate");
                var modificationDateField = new DataField<DateTime>("ModificationDate");
                var applicationDeadlineDateField = new DataField<DateTime>("ApplicationDeadlineDate");
                var jobDescriptionEnField = new DataField<string>("JobDescriptionEn");
                var jobDescriptionFrField = new DataField<string>("JobDescriptionFr");
                var workscheduleIdField = new DataField<string>("WorkScheduleId");
                var workscheduleField = new DataField<string>("WorkSchedule");
                var securityClearanceIdField = new DataField<string>("SecurityClearanceId");
                var securityClearanceField = new DataField<string>("SecurityClearance");
                var languageComprehensionField = new DataField<string?>("LanguageComprehension");
                var languageRequirementIdField = new DataField<string>("LanguageRequirementId");
                var languageRequirementField = new DataField<string>("LanguageRequirement");
                var workArrangementIdField = new DataField<string>("WorkArrangementId");
                var workArrangementField = new DataField<string>("WorkArrangement");
                var approvedStaffindField = new DataField<bool>("ApprovedStaffing");
                var cityIdField = new DataField<string>("CityId");
                var cityField = new DataField<string>("City");
                var regionIdField = new DataField<string>("RegionId");
                var regionField = new DataField<string>("Region");
                var provinceIdField = new DataField<string>("ProvinceId");
                var provinceField = new DataField<string>("Province");
                var programAreaIdField = new DataField<string>("ProgramAreaId");
                var programAreaField = new DataField<string>("ProgramArea");
                var snapshotDateField = new DataField<DateTime>("SnapshotDate");

                // TODO: Update schema
                var schema = new ParquetSchema(idField, authorMailField, departmentIdField, titleEnField, titleFrField, classCodeIdField, classLevelIdField, numOpportunitiesField,
                    durationIdField, durationQuantityField, creationDateField, modificationDateField, applicationDeadlineDateField, jobDescriptionEnField, jobDescriptionFrField,
                    workscheduleIdField, securityClearanceIdField, languageComprehensionField, languageRequirementIdField, workArrangementIdField, approvedStaffindField,
                    cityIdField, /*programAreaIdField,*/ snapshotDateField);

                await using var parquetWriterJobOpp = await ParquetWriter.CreateAsync(schema, blobStreamJobOpp, Globals.ParquetOptions);

                // BUFFERS
                var idBuffer = new List<string>(Globals.RowGroupBatchSize);
                var authorMailBuffer = new List<string>(Globals.RowGroupBatchSize);
                var departmentIdBuffer = new List<string>(Globals.RowGroupBatchSize);
                var departmentBuffer = new List<string>(Globals.RowGroupBatchSize);
                var titleEnBuffer = new List<string>(Globals.RowGroupBatchSize);
                var titleFrBuffer = new List<string>(Globals.RowGroupBatchSize);
                var classCodeIdBuffer = new List<string>(Globals.RowGroupBatchSize);
                var classCodeBuffer = new List<string>(Globals.RowGroupBatchSize);
                var classLevelIdBuffer = new List<string>(Globals.RowGroupBatchSize);
                var classLevelBuffer = new List<string>(Globals.RowGroupBatchSize);
                var numOpportunitiesBuffer = new List<int>(Globals.RowGroupBatchSize);
                var durationIdBuffer = new List<string?>(Globals.RowGroupBatchSize);
                var durationBuffer = new List<string?>(Globals.RowGroupBatchSize);
                var durationQuantityBuffer = new List<double>(Globals.RowGroupBatchSize);
                var creationDateBuffer = new List<DateTime>(Globals.RowGroupBatchSize);
                var modificationDateBuffer = new List<DateTime>(Globals.RowGroupBatchSize);
                var applicationDeadlineDateBuffer = new List<DateTime>(Globals.RowGroupBatchSize);
                var jobDescriptionEnBuffer = new List<string>(Globals.RowGroupBatchSize);
                var jobDescriptionFrBuffer = new List<string>(Globals.RowGroupBatchSize);
                var workscheduleIdBuffer = new List<string>(Globals.RowGroupBatchSize);
                var workscheduleBuffer = new List<string>(Globals.RowGroupBatchSize);
                var securityClearanceIdBuffer = new List<string>(Globals.RowGroupBatchSize);
                var securityClearanceBuffer = new List<string>(Globals.RowGroupBatchSize);
                var languageComprehensionBuffer = new List<string?>(Globals.RowGroupBatchSize);
                var languageRequirementIdBuffer = new List<string>(Globals.RowGroupBatchSize);
                var languageRequirementBuffer = new List<string>(Globals.RowGroupBatchSize);
                var workArrangementIdBuffer = new List<string>(Globals.RowGroupBatchSize);
                var workArrangementBuffer = new List<string>(Globals.RowGroupBatchSize);
                var approvedStaffindBuffer = new List<bool>(Globals.RowGroupBatchSize);
                var cityIdBuffer = new List<string>(Globals.RowGroupBatchSize);
                var cityBuffer = new List<string>(Globals.RowGroupBatchSize);
                var regionIdBuffer = new List<string>(Globals.RowGroupBatchSize);
                var regionBuffer = new List<string>(Globals.RowGroupBatchSize);
                var provinceIdBuffer = new List<string>(Globals.RowGroupBatchSize);
                var provinceBuffer = new List<string>(Globals.RowGroupBatchSize);
                var programAreaIdBuffer = new List<string>(Globals.RowGroupBatchSize);
                var programAreaBuffer = new List<string>(Globals.RowGroupBatchSize);
                var snapshotDateBuffer = new List<DateTime>(Globals.RowGroupBatchSize);

                async Task FlushBatchAsync()
                {
                    if (idBuffer.Count == 0)
                        return;

                    using var groupWriter = parquetWriterJobOpp.CreateRowGroup();

                    await groupWriter.WriteAsync(idField, idBuffer);
                    await groupWriter.WriteAsync(authorMailField, authorMailBuffer);
                    await groupWriter.WriteAsync(departmentIdField, departmentIdBuffer);
                    await groupWriter.WriteAsync(departmentField, departmentBuffer);
                    await groupWriter.WriteAsync(titleEnField, titleEnBuffer);
                    await groupWriter.WriteAsync(titleFrField, titleFrBuffer);
                    await groupWriter.WriteAsync(classCodeIdField, classCodeIdBuffer);
                    await groupWriter.WriteAsync(classCodeField, classCodeBuffer);
                    await groupWriter.WriteAsync(classLevelIdField, classLevelIdBuffer);
                    await groupWriter.WriteAsync(classLevelField, classLevelBuffer);
                    await groupWriter.WriteAsync<int>(numOpportunitiesField, numOpportunitiesBuffer.ToArray().AsMemory());
                    await groupWriter.WriteAsync(durationIdField, durationIdBuffer);
                    await groupWriter.WriteAsync(durationField, durationBuffer);
                    await groupWriter.WriteAsync<double>(durationQuantityField, durationQuantityBuffer.ToArray().AsMemory());
                    await groupWriter.WriteAsync<DateTime>(creationDateField, creationDateBuffer.ToArray().AsMemory());
                    await groupWriter.WriteAsync<DateTime>(modificationDateField, modificationDateBuffer.ToArray().AsMemory());
                    await groupWriter.WriteAsync<DateTime>(applicationDeadlineDateField, applicationDeadlineDateBuffer.ToArray().AsMemory());
                    await groupWriter.WriteAsync(jobDescriptionEnField, jobDescriptionEnBuffer);
                    await groupWriter.WriteAsync(jobDescriptionFrField, jobDescriptionFrBuffer);
                    await groupWriter.WriteAsync(workscheduleIdField, workscheduleIdBuffer);
                    await groupWriter.WriteAsync(workscheduleField, workscheduleBuffer);
                    await groupWriter.WriteAsync(securityClearanceIdField, securityClearanceIdBuffer);
                    await groupWriter.WriteAsync(securityClearanceField, securityClearanceBuffer);
                    await groupWriter.WriteAsync(languageComprehensionField, languageComprehensionBuffer);
                    await groupWriter.WriteAsync(languageRequirementIdField, languageRequirementIdBuffer);
                    await groupWriter.WriteAsync(languageRequirementField, languageRequirementBuffer);
                    await groupWriter.WriteAsync(workArrangementIdField, workArrangementIdBuffer);
                    await groupWriter.WriteAsync(workArrangementField, workArrangementBuffer);
                    await groupWriter.WriteAsync<bool>(approvedStaffindField, approvedStaffindBuffer.ToArray().AsMemory());
                    await groupWriter.WriteAsync(cityIdField, cityIdBuffer);
                    await groupWriter.WriteAsync(cityField, cityBuffer);
                    await groupWriter.WriteAsync(regionIdField, regionIdBuffer);
                    await groupWriter.WriteAsync(regionField, regionBuffer);
                    await groupWriter.WriteAsync(provinceIdField, provinceIdBuffer);
                    await groupWriter.WriteAsync(provinceField, provinceBuffer);
                    await groupWriter.WriteAsync(programAreaIdField, programAreaIdBuffer);
                    await groupWriter.WriteAsync(programAreaField, programAreaBuffer);
                    await groupWriter.WriteAsync<DateTime>(snapshotDateField, snapshotDateBuffer.ToArray().AsMemory());

                    idBuffer.Clear();
                    authorMailBuffer.Clear();
                    departmentIdBuffer.Clear();
                    departmentBuffer.Clear();
                    titleEnBuffer.Clear();
                    titleFrBuffer.Clear();
                    classCodeIdBuffer.Clear();
                    classCodeBuffer.Clear();
                    classLevelIdBuffer.Clear();
                    classLevelBuffer.Clear();
                    numOpportunitiesBuffer.Clear();
                    durationIdBuffer.Clear();
                    durationBuffer.Clear();
                    durationQuantityBuffer.Clear();
                    creationDateBuffer.Clear();
                    modificationDateBuffer.Clear();
                    applicationDeadlineDateBuffer.Clear();
                    jobDescriptionEnBuffer.Clear();
                    jobDescriptionFrBuffer.Clear();
                    workscheduleIdBuffer.Clear();
                    workscheduleBuffer.Clear();
                    securityClearanceIdBuffer.Clear();
                    securityClearanceBuffer.Clear();
                    languageComprehensionBuffer.Clear();
                    languageRequirementIdBuffer.Clear();
                    languageRequirementBuffer.Clear();
                    workArrangementIdBuffer.Clear();
                    workArrangementBuffer.Clear();
                    approvedStaffindBuffer.Clear();
                    cityIdBuffer.Clear();
                    cityBuffer.Clear();
                    regionIdBuffer.Clear();
                    regionBuffer.Clear();
                    provinceIdBuffer.Clear();
                    provinceBuffer.Clear();
                    programAreaIdBuffer.Clear();
                    programAreaBuffer.Clear();
                    snapshotDateBuffer.Clear();
                }

                var siteId = Auth.GetAppSetting("cmSiteId", _logger, _config);
                var listIdJobOpp = Auth.GetAppSetting("cmJobOpportunityListId", _logger, _config);

                // Cache all the lookup lists
                var departments = await LoadLookupValuesAsync(graphClient, siteId, Auth.GetAppSetting("cmDepartmentListId", _logger, _config));
                var classificationCodes = await LoadLookupValuesAsync(graphClient, siteId, Auth.GetAppSetting("cmClassificationCodeListId", _logger, _config));
                var classificationLevels = await LoadLookupValuesAsync(graphClient, siteId, Auth.GetAppSetting("cmClassificationLevelListId", _logger, _config));
                var durations = await LoadLookupValuesAsync(graphClient, siteId, Auth.GetAppSetting("cmDurationListId", _logger, _config));
                var workSchedules = await LoadLookupValuesAsync(graphClient, siteId, Auth.GetAppSetting("cmWorkScheduleListId", _logger, _config));
                var securityClearances = await LoadLookupValuesAsync(graphClient, siteId, Auth.GetAppSetting("cmSecurityClearanceListId", _logger, _config));
                var languageRequirements = await LoadLookupValuesAsync(graphClient, siteId, Auth.GetAppSetting("cmLanguageRequirementListId", _logger, _config));
                var workArrangements = await LoadLookupValuesAsync(graphClient, siteId, Auth.GetAppSetting("cmWorkArrangementListId", _logger, _config));
                var cities = await LoadLookupValuesAsync(graphClient, siteId, Auth.GetAppSetting("cmCityListId", _logger, _config));
                var regions = await LoadLookupValuesAsync(graphClient, siteId, Auth.GetAppSetting("cmRegionListId", _logger, _config));
                var provinces = await LoadLookupValuesAsync(graphClient, siteId, Auth.GetAppSetting("cmProvinceListId", _logger, _config));
                var skills = await LoadLookupValuesAsync(graphClient, siteId, Auth.GetAppSetting("cmSkillsListId", _logger, _config));
                // TODO: Cache the term sets (ProgramArea and jobType)

                var jobOpportunityPage = await graphClient.Sites[siteId].Lists[listIdJobOpp].Items.GetAsync(rc =>
                {
                    rc.QueryParameters.Expand = new[] { "fields" };
                });

                var countJobOpp = 0;
                
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
                                authorMailBuffer.Add(Globals.GetField(fields, "ContactEmail"));

                                var departmentId = Globals.GetField(fields, "DepartmentLookupId");
                                departmentIdBuffer.Add(departmentId);
                                departmentBuffer.Add(departments[departmentId]);

                                titleEnBuffer.Add(Globals.GetField(fields, "JobTitleEn"));
                                titleFrBuffer.Add(Globals.GetField(fields, "JobTitleFr"));

                                var classCodeId = Globals.GetField(fields, "ClassificationCodeLookupId");
                                classCodeIdBuffer.Add(classCodeId);
                                classCodeBuffer.Add(classificationCodes[classCodeId]);

                                var classLevelId = Globals.GetField(fields, "ClassificationLevelLookupId");
                                classLevelIdBuffer.Add(classLevelId);
                                classLevelBuffer.Add(classificationLevels[classLevelId]);

                                numOpportunitiesBuffer.Add(Convert.ToInt32(Convert.ToDouble(Globals.GetField(fields, "NumberOfOpportunities"))));

                                var durationId = Globals.GetFieldOrNull(fields, "DurationLookupId");
                                durationIdBuffer.Add(durationId);
                                durationBuffer.Add(durationId != null ? durations[durationId] : null);

                                durationQuantityBuffer.Add(Convert.ToDouble(Globals.GetField(fields, "DurationQuantity")));
                                creationDateBuffer.Add(Convert.ToDateTime(Globals.GetField(fields, "Created")));
                                modificationDateBuffer.Add(Convert.ToDateTime(Globals.GetField(fields, "Modified")));
                                applicationDeadlineDateBuffer.Add(Convert.ToDateTime(Globals.GetField(fields, "ApplicationDeadlineDate")));
                                jobDescriptionEnBuffer.Add(Globals.GetField(fields, "JobDescriptionEn"));
                                jobDescriptionFrBuffer.Add(Globals.GetField(fields, "JobDescriptionFr"));

                                var workScheduleId = Globals.GetField(fields, "WorkScheduleLookupId");
                                workscheduleIdBuffer.Add(workScheduleId);
                                workscheduleBuffer.Add(workSchedules[workScheduleId]);

                                var securityClearanceId = Globals.GetField(fields, "SecurityClearanceLookupId");
                                securityClearanceIdBuffer.Add(securityClearanceId);
                                securityClearanceBuffer.Add(securityClearances[securityClearanceId]);

                                languageComprehensionBuffer.Add(Globals.GetField(fields, "LanguageComprehension"));

                                var languageRequirementId = Globals.GetField(fields, "LanguageRequirementLookupId");
                                languageRequirementIdBuffer.Add(languageRequirementId);
                                languageRequirementBuffer.Add(languageRequirements[languageRequirementId]);

                                var workArrangementId = Globals.GetField(fields, "WorkArrangementLookupId");
                                workArrangementIdBuffer.Add(workArrangementId);
                                workArrangementBuffer.Add(workArrangements[workArrangementId]);

                                approvedStaffindBuffer.Add(Convert.ToBoolean(Globals.GetField(fields, "ApprovedStaffing")));

                                var cityId = Globals.GetField(fields, "CityLookupId");
                                cityIdBuffer.Add(cityId);
                                cityBuffer.Add(cities[cityId]);

                                // TODO: Region / Province

                                //programAreaIdBuffer.Add(); // This is a term
                                snapshotDateBuffer.Add(snapshotDate);

                                if (idBuffer.Count >= Globals.RowGroupBatchSize)
                                {
                                    FlushBatchAsync().GetAwaiter().GetResult();
                                }

                                countJobOpp++;
                            }
                            return true;
                        });

                await pageIterator.IterateAsync();
                await FlushBatchAsync();
                await parquetWriterJobOpp.DisposeAsync();

                _logger.LogInformation($"Streamed {countJobOpp} job opportunities to {blobNameJobOpportunities}");

                return blobNameJobOpportunities;
            }
            catch (Exception ex)
            {
                _logger.LogError("StreamJobOpportunitiesToBlobAsync failed");
                _logger.LogError(ex.Message);
                throw;
            }
        }

        private async Task<Dictionary<string, string?>> LoadLookupValuesAsync(GraphServiceClient graphClient, string siteId, string listId)
        {
            var values = new Dictionary<string, string?>();

            var response = await graphClient.Sites[siteId].Lists[listId].Items.GetAsync(rc =>
            {
                rc.QueryParameters.Expand = new[] { "fields" };
            });

            if (response == null)
                return values;

            var pageIterator = PageIterator<ListItem, ListItemCollectionResponse>
                .CreatePageIterator(
                    graphClient,
                    response,
                    item =>
                    {
                        var fields = item.Fields?.AdditionalData;

                        if (item.Id != null && fields != null)
                        {
                            string? value = null;

                            // Look for NameEn or TitleEn
                            foreach (var field in fields)
                            {
                                if (field.Key.Equals("NameEn",StringComparison.OrdinalIgnoreCase) || field.Key.Equals("TitleEn",StringComparison.OrdinalIgnoreCase))
                                {
                                    value = field.Value?.ToString();
                                    break;
                                }
                            }

                            values[item.Id] = value;
                        }

                        return true;
                    });

            await pageIterator.IterateAsync();

            return values;
        }
    }
}
