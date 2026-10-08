using Microsoft.AspNetCore.Http;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Graph;
using Microsoft.Graph.Models;
using Microsoft.Graph.Models.TermStore;
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

        private record LookupValue(string Id, string? Title, string? ParentId);

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

                var blobClientJobOpp = await Auth.GetBlobClient(JobOpportunitiesContainerName, blobNameJobOpportunities, _logger, _config);
                var blobClientSkills = await Auth.GetBlobClient(JobOpportunitySkillsContainerName, blobNameSkills, _logger, _config);
                var blobClientJobType = await Auth.GetBlobClient(JobOpportunityJobTypesContainerName, blobNameJobTypes, _logger, _config);

                using var blobStreamJobOpp = await blobClientJobOpp.OpenWriteAsync(overwrite: true);
                using var blobStreamSkills = await blobClientSkills.OpenWriteAsync(overwrite: true);
                using var blobStreamJobType = await blobClientJobType.OpenWriteAsync(overwrite: true);

                // JobOpportunity fields
                var idField = new DataField<string>("Id");
                var authorMailField = new DataField<string>("AuthorMail");
                var departmentIdField = new DataField<string>("DepartmentId");
                var departmentField = new DataField<string>("Department");
                var titleEnField = new DataField<string>("JobTitleEn");
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
                var cityIdField = new DataField<string?>("CityId");
                var cityField = new DataField<string?>("City");
                var regionIdField = new DataField<string?>("RegionId");
                var regionField = new DataField<string?>("Region");
                var provinceIdField = new DataField<string?>("ProvinceId");
                var provinceField = new DataField<string?>("Province");
                var programAreaIdField = new DataField<string?>("ProgramAreaId");
                var programAreaField = new DataField<string?>("ProgramArea");
                var snapshotDateField = new DataField<DateTime>("SnapshotDate");

                // Skill/JobType fields
                var jobOpportunityIdField = new DataField<string>("JobOpportunityId");
                var skillIdField = new DataField<string>("SkillId");
                var jobTypeIdField = new DataField<string>("JobTypeId");
                var titleField = new DataField<string>("Title");

                var schemaJobOpp = new ParquetSchema(idField, authorMailField, departmentIdField, departmentField, titleEnField, classCodeIdField, classCodeField, classLevelIdField,
                    classLevelField, numOpportunitiesField, durationIdField, durationField, durationQuantityField, creationDateField, modificationDateField, applicationDeadlineDateField,
                    workscheduleIdField, workscheduleField, securityClearanceIdField, securityClearanceField, languageComprehensionField, languageRequirementIdField, languageRequirementField,
                    workArrangementIdField, workArrangementField, approvedStaffindField, cityIdField, cityField, regionIdField, regionField, provinceIdField, provinceField, programAreaIdField,
                    programAreaField, snapshotDateField);
                await using var parquetWriterJobOpp = await ParquetWriter.CreateAsync(schemaJobOpp, blobStreamJobOpp, Globals.ParquetOptions);

                var schemaSkills = new ParquetSchema(jobOpportunityIdField, skillIdField, titleField, snapshotDateField);
                await using var parquetWriterSkills = await ParquetWriter.CreateAsync(schemaSkills, blobStreamSkills, Globals.ParquetOptions);

                var schemaJobType = new ParquetSchema(jobOpportunityIdField, jobTypeIdField, titleField, snapshotDateField);
                await using var parquetWriterJobType= await ParquetWriter.CreateAsync(schemaJobType, blobStreamJobType, Globals.ParquetOptions);

                // JobOpportunity buffers
                var idBuffer = new List<string>(Globals.RowGroupBatchSize);
                var authorMailBuffer = new List<string>(Globals.RowGroupBatchSize);
                var departmentIdBuffer = new List<string>(Globals.RowGroupBatchSize);
                var departmentBuffer = new List<string>(Globals.RowGroupBatchSize);
                var titleEnBuffer = new List<string>(Globals.RowGroupBatchSize);
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
                var cityIdBuffer = new List<string?>(Globals.RowGroupBatchSize);
                var cityBuffer = new List<string?>(Globals.RowGroupBatchSize);
                var regionIdBuffer = new List<string?>(Globals.RowGroupBatchSize);
                var regionBuffer = new List<string?>(Globals.RowGroupBatchSize);
                var provinceIdBuffer = new List<string?>(Globals.RowGroupBatchSize);
                var provinceBuffer = new List<string?>(Globals.RowGroupBatchSize);
                var programAreaIdBuffer = new List<string?>(Globals.RowGroupBatchSize);
                var programAreaBuffer = new List<string?>(Globals.RowGroupBatchSize);
                var snapshotDateBuffer = new List<DateTime>(Globals.RowGroupBatchSize);

                async Task FlushJobOpportunityBatchAsync()
                {
                    if (idBuffer.Count == 0)
                        return;

                    using var groupWriter = parquetWriterJobOpp.CreateRowGroup();

                    await groupWriter.WriteAsync(idField, idBuffer);
                    await groupWriter.WriteAsync(authorMailField, authorMailBuffer);
                    await groupWriter.WriteAsync(departmentIdField, departmentIdBuffer);
                    await groupWriter.WriteAsync(departmentField, departmentBuffer);
                    await groupWriter.WriteAsync(titleEnField, titleEnBuffer);
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

                // Skills buffers
                var jobOpportunityIdSkillsBuffer = new List<string>(Globals.RowGroupBatchSize);
                var skillIdBuffer = new List<string>(Globals.RowGroupBatchSize);
                var titleSkillsBuffer = new List<string>(Globals.RowGroupBatchSize);
                var snapshotDateSkillsBuffer = new List<DateTime>(Globals.RowGroupBatchSize);

                async Task FlushSkillsBatchAsync()
                {
                    if (jobOpportunityIdSkillsBuffer.Count == 0)
                        return;

                    using var groupWriter = parquetWriterSkills.CreateRowGroup();

                    await groupWriter.WriteAsync(jobOpportunityIdField, jobOpportunityIdSkillsBuffer);
                    await groupWriter.WriteAsync(skillIdField, skillIdBuffer);
                    await groupWriter.WriteAsync(titleField, titleSkillsBuffer);
                    await groupWriter.WriteAsync<DateTime>(snapshotDateField, snapshotDateSkillsBuffer.ToArray().AsMemory());

                    jobOpportunityIdSkillsBuffer.Clear();
                    skillIdBuffer.Clear();
                    titleSkillsBuffer.Clear();
                    snapshotDateSkillsBuffer.Clear();
                }

                // JobType buffers
                var jobOpportunityIdJobTypeBuffer = new List<string>(Globals.RowGroupBatchSize);
                var jobTypeIdBuffer = new List<string>(Globals.RowGroupBatchSize);
                var titleJobTypeBuffer = new List<string>(Globals.RowGroupBatchSize);
                var snapshotDateJobTypeBuffer = new List<DateTime>(Globals.RowGroupBatchSize);

                async Task FlushJobTypeBatchAsync()
                {
                    if (jobOpportunityIdJobTypeBuffer.Count == 0)
                        return;

                    using var groupWriter = parquetWriterJobType.CreateRowGroup();

                    await groupWriter.WriteAsync(jobOpportunityIdField, jobOpportunityIdJobTypeBuffer);
                    await groupWriter.WriteAsync(jobTypeIdField, jobTypeIdBuffer);
                    await groupWriter.WriteAsync(titleField, titleJobTypeBuffer);
                    await groupWriter.WriteAsync<DateTime>(snapshotDateField, snapshotDateJobTypeBuffer.ToArray().AsMemory());

                    jobOpportunityIdJobTypeBuffer.Clear();
                    jobTypeIdBuffer.Clear();
                    titleJobTypeBuffer.Clear();
                    snapshotDateJobTypeBuffer.Clear();
                }

                var graphClient = Auth.GetGraphServiceClient(_logger);

                var siteId = Auth.GetAppSetting("cmSiteId", _logger, _config);
                var listIdJobOpp = Auth.GetAppSetting("cmJobOpportunityListId", _logger, _config);

                // Cache the lookup lists
                var departments = await LoadLookupValuesAsync(graphClient, siteId, Auth.GetAppSetting("cmDepartmentListId", _logger, _config));
                var classificationCodes = await LoadLookupValuesAsync(graphClient, siteId, Auth.GetAppSetting("cmClassificationCodeListId", _logger, _config));
                var classificationLevels = await LoadLookupValuesAsync(graphClient, siteId, Auth.GetAppSetting("cmClassificationLevelListId", _logger, _config));
                var durations = await LoadLookupValuesAsync(graphClient, siteId, Auth.GetAppSetting("cmDurationListId", _logger, _config));
                var workSchedules = await LoadLookupValuesAsync(graphClient, siteId, Auth.GetAppSetting("cmWorkScheduleListId", _logger, _config));
                var securityClearances = await LoadLookupValuesAsync(graphClient, siteId, Auth.GetAppSetting("cmSecurityClearanceListId", _logger, _config));
                var languageRequirements = await LoadLookupValuesAsync(graphClient, siteId, Auth.GetAppSetting("cmLanguageRequirementListId", _logger, _config));
                var workArrangements = await LoadLookupValuesAsync(graphClient, siteId, Auth.GetAppSetting("cmWorkArrangementListId", _logger, _config));
                var cities = await LoadLookupValuesAsync(graphClient, siteId, Auth.GetAppSetting("cmCityListId", _logger, _config), new[] { "NameEn", "TitleEn", "RegionLookupId" } );
                var regions = await LoadLookupValuesAsync(graphClient, siteId, Auth.GetAppSetting("cmRegionListId", _logger, _config), new[] { "NameEn", "TitleEn", "ProvinceLookupId" });
                var provinces = await LoadLookupValuesAsync(graphClient, siteId, Auth.GetAppSetting("cmProvinceListId", _logger, _config), new[] { "NameEn", "TitleEn" });
                var skills = await LoadLookupValuesAsync(graphClient, siteId, Auth.GetAppSetting("cmSkillsListId", _logger, _config));

                // Cache the term sets
                var jobTypes = await LoadTermSetValuesAsync(graphClient, "root", Auth.GetAppSetting("cmJobTypeTermSetId", _logger, _config));
                var programAreas = await LoadTermSetValuesAsync(graphClient, "root", Auth.GetAppSetting("cmProgramAreaTermSetId", _logger, _config));

                // Get all the job opportunities
                //var jobOpportunityPage = await graphClient.Sites[siteId].Lists[listIdJobOpp].Items.GetAsync(rc =>
                //{
                //    // TODO: Specifcy every field
                //    rc.QueryParameters.Expand = new[] 
                //    { 
                //        "fields" 
                //    };
                //});

                var selectColumns = new[]
                {
                    "ContactEmail",
                    "DepartmentLookupId",
                    "JobTitleEn",
                    "ClassificationCodeLookupId",
                    "ClassificationLevelLookupId",
                    "NumberOfOpportunities",
                    "DurationLookupId",          // was misspelled "DuratiopnLookupId"
                    "DurationQuantity",
                    "Created",
                    "Modified",
                    "ApplicationDeadlineDate",
                    "WorkScheduleLookupId",
                    "SecurityClearanceLookupId",
                    "LanguageComprehension",
                    "LanguageRequirementLookupId",
                    "WorkArrangementLookupId",
                    "ApprovedStaffing",
                    "CityLookupId",
                    "Skills",            // multi-value lookup IDs
                    "JobType",
                    "JobType_0",
                    "ProgramArea",
                    "ProgramArea_0"
                };

                var jobOpportunityPage = await graphClient.Sites[siteId].Lists[listIdJobOpp].Items.GetAsync(rc =>
                {
                    rc.QueryParameters.Expand = new[] { $"fields($select={string.Join(",", selectColumns)})" };
                });

                var countJobOpp = 0;
                var countSkills = 0;
                var countJobType = 0;
                
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
                                departmentBuffer.Add(departments.Where(item => item.Id == departmentId).FirstOrDefault()?.Title!);

                                titleEnBuffer.Add(Globals.GetField(fields, "JobTitleEn"));

                                var classCodeId = Globals.GetField(fields, "ClassificationCodeLookupId");
                                classCodeIdBuffer.Add(classCodeId);
                                classCodeBuffer.Add(classificationCodes.Where(item => item.Id == classCodeId).FirstOrDefault()?.Title!);

                                var classLevelId = Globals.GetField(fields, "ClassificationLevelLookupId");
                                classLevelIdBuffer.Add(classLevelId);
                                classLevelBuffer.Add(classificationLevels.Where(item => item.Id == classLevelId).FirstOrDefault()?.Title!);

                                numOpportunitiesBuffer.Add(Convert.ToInt32(Convert.ToDouble(Globals.GetField(fields, "NumberOfOpportunities"))));

                                var durationId = Globals.GetFieldOrNull(fields, "DurationLookupId");
                                durationIdBuffer.Add(durationId);
                                durationBuffer.Add(durationId != null ? durations.Where(item => item.Id == durationId).FirstOrDefault()?.Title! : null);

                                durationQuantityBuffer.Add(Convert.ToDouble(Globals.GetField(fields, "DurationQuantity")));
                                creationDateBuffer.Add(Convert.ToDateTime(Globals.GetField(fields, "Created")));
                                modificationDateBuffer.Add(Convert.ToDateTime(Globals.GetField(fields, "Modified")));
                                applicationDeadlineDateBuffer.Add(Convert.ToDateTime(Globals.GetField(fields, "ApplicationDeadlineDate")));

                                var workScheduleId = Globals.GetField(fields, "WorkScheduleLookupId");
                                workscheduleIdBuffer.Add(workScheduleId);
                                workscheduleBuffer.Add(workSchedules.Where(item => item.Id == workScheduleId).FirstOrDefault()?.Title!);

                                var securityClearanceId = Globals.GetField(fields, "SecurityClearanceLookupId");
                                securityClearanceIdBuffer.Add(securityClearanceId);
                                securityClearanceBuffer.Add(securityClearances.Where(item => item.Id == securityClearanceId).FirstOrDefault()?.Title!);

                                languageComprehensionBuffer.Add(Globals.GetField(fields, "LanguageComprehension"));

                                var languageRequirementId = Globals.GetField(fields, "LanguageRequirementLookupId");
                                languageRequirementIdBuffer.Add(languageRequirementId);
                                languageRequirementBuffer.Add(languageRequirements.Where(item => item.Id == languageRequirementId).FirstOrDefault()?.Title!);

                                var workArrangementId = Globals.GetField(fields, "WorkArrangementLookupId");
                                workArrangementIdBuffer.Add(workArrangementId);
                                workArrangementBuffer.Add(workArrangements.Where(item => item.Id == workArrangementId).FirstOrDefault()?.Title!);

                                approvedStaffindBuffer.Add(Convert.ToBoolean(Globals.GetField(fields, "ApprovedStaffing")));

                                var cityId = Globals.GetFieldOrNull(fields, "CityLookupId");
                                cityIdBuffer.Add(cityId);
                                cityBuffer.Add(cityId != null ? cities.Where(item => item.Id == cityId).FirstOrDefault()?.Title : null);

                                var regionId = cityId != null ? cities.Where(city => city.Id == cityId).FirstOrDefault()?.ParentId : null;
                                regionIdBuffer.Add(regionId);
                                regionBuffer.Add(regionId != null ? regions.Where(item => item.Id == regionId).FirstOrDefault()?.Title : null);

                                var provinceId = regionId != null ? regions.Where(region => region.Id == regionId).FirstOrDefault()?.ParentId : null;
                                provinceIdBuffer.Add(provinceId);
                                provinceBuffer.Add(provinceId != null ? provinces.Where(item => item.Id == cityId).FirstOrDefault()?.Title : null);

                                var contactEmail = Globals.GetField(fields, "ContactEmail");
                                var jobTitle = Globals.GetField(fields, "JobTitleEn");

                                var programAreaId = Globals.GetFieldOrNull(fields, "ProgramAreaId");
                                programAreaIdBuffer.Add(programAreaId);
                                programAreaBuffer.Add(programAreaId != null ? programAreas[programAreaId] : null);

                                snapshotDateBuffer.Add(snapshotDate);

                                if (idBuffer.Count >= Globals.RowGroupBatchSize)
                                {
                                    FlushJobOpportunityBatchAsync().GetAwaiter().GetResult();
                                }

                                countJobOpp++;
                            }
                            return true;
                        });

                await pageIterator.IterateAsync();

                await FlushJobOpportunityBatchAsync();
                await FlushSkillsBatchAsync();
                await FlushJobTypeBatchAsync();

                await parquetWriterJobOpp.DisposeAsync();
                await parquetWriterSkills.DisposeAsync();
                await parquetWriterJobType.DisposeAsync();

                _logger.LogInformation($"Streamed {countJobOpp} job opportunities to {blobNameJobOpportunities}");
                _logger.LogInformation($"Streamed {countSkills} job opportunities to {blobNameSkills}");
                _logger.LogInformation($"Streamed {countJobType} job opportunities to {blobNameJobTypes}");

                return blobNameJobOpportunities;
            }
            catch (Exception ex)
            {
                _logger.LogError("StreamJobOpportunitiesToBlobAsync failed");
                _logger.LogError(ex.Message);
                throw;
            }
        }

        private async Task<List<LookupValue>> LoadLookupValuesAsync(GraphServiceClient graphClient, string siteId, string listId, string[]? selectColumns = null)
        {
            var values = new List<LookupValue>();

            var response = await graphClient.Sites[siteId].Lists[listId].Items.GetAsync(rc =>
            {
                rc.QueryParameters.Expand = selectColumns is null || selectColumns.Length == 0 ? ["fields"] : [$"fields($select={string.Join(",", selectColumns)})"];
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
                            string? title = null;
                            string? parentId = null;

                            foreach (var field in fields)
                            {
                                if (title != null && parentId != null)
                                    break;

                                if (field.Key.Equals("NameEn", StringComparison.OrdinalIgnoreCase) || field.Key.Equals("TitleEn", StringComparison.OrdinalIgnoreCase))
                                {
                                    title = field.Value?.ToString();
                                }
                                else if (field.Key.Equals("RegionLookupId", StringComparison.OrdinalIgnoreCase) || field.Key.Equals("ProvinceLookupId", StringComparison.OrdinalIgnoreCase))
                                {
                                    parentId = field.Value?.ToString();
                                }
                            }

                            values.Add(new LookupValue(item.Id, title, parentId));
                        }

                        return true;
                    });

            await pageIterator.IterateAsync();

            return values;
        }

        private async Task<Dictionary<string, string?>> LoadTermSetValuesAsync(GraphServiceClient graphClient, string siteId, string termSetId)
        {
            var values = new Dictionary<string, string?>();

            var response = await graphClient.Sites[siteId].TermStore.Sets[termSetId].Terms.GetAsync();

            if (response == null)
                return values;

            var pageIterator = PageIterator<Term, TermCollectionResponse>
                .CreatePageIterator(
                    graphClient,
                    response,
                    term =>
                    {
                        if (term.Id != null && term.Labels != null)
                        {
                            string? value = null;

                            foreach (var label in term.Labels)
                            {
                                if (label.LanguageTag!.Equals("en-us", StringComparison.OrdinalIgnoreCase))
                                {
                                    value = label.Name?.ToString();
                                    break;
                                }
                            }

                            values[term.Id] = value;
                        }

                        return true;
                    });

            await pageIterator.IterateAsync();

            return values;
        }
    }
}
