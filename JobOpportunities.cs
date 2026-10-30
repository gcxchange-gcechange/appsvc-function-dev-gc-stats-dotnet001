using Azure.Identity;
using Azure.Storage.Blobs;
using GCStats.Models;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Graph;
using Microsoft.Graph.Models;
using Microsoft.Kiota.Abstractions.Serialization;
using Newtonsoft.Json;
using Parquet;
using Parquet.Schema;
using System.Collections.Concurrent;
using System.Globalization;

namespace GCStats
{
    public class JobOpportunities : IJob
    {
        private readonly ILogger<JobOpportunities> _logger;
        private readonly IConfiguration _config;
        private readonly JobQueue _queue;

        public string Name => "JobOpportunities";
        public const string JobOpportunitiesContainerName = "job-opportunities";
        public const string JobOpportunitySkillsContainerName = "job-opportunity-skills";
        public const string JobOpportunityJobTypesContainerName = "job-opportunity-job-types";

        private record LookupValue(string Id, string? Title, string? ParentId);
        private string ProgramAreaColumnName = "Program_Area"; // TODO: Make this an app setting?

        public JobOpportunities(ILogger<JobOpportunities> logger, IConfiguration config, JobQueue queue)
        {
            _logger = logger;
            _config = config;
            _queue = queue;
        }

        [Function("Timer_JobOpportunities")]
        public Task Schedule([TimerTrigger(Globals.TimerStartTime)] TimerInfo timer) => _queue.EnqueueJobAsync(Name);

        public async Task RunAsync(CancellationToken ct)
        {
            _logger.LogInformation($"{Name} executed at: {DateTime.UtcNow}");

            var blobName = await StreamJobOpportunitiesToBlobAsync(ct);

            _logger.LogInformation($"BlobName: {blobName}");

            await _queue.SendAsync("process-job-opportunities", blobName);
        }

        public async Task<string> StreamJobOpportunitiesToBlobAsync(CancellationToken ct = default)
        {
            var blobNameJobOpportunities = $"{JobOpportunitiesContainerName}-{DateTime.UtcNow.ToString(Globals.BlobDateFormat)}.parquet";
            var blobNameSkills = $"{JobOpportunitySkillsContainerName}-{DateTime.UtcNow.ToString(Globals.BlobDateFormat)}.parquet";
            var blobNameJobTypes = $"{JobOpportunityJobTypesContainerName}-{DateTime.UtcNow.ToString(Globals.BlobDateFormat)}.parquet";

            try
            {
                var snapshotDate = DateTime.UtcNow.Date;

                var blobClientJobOpp = await Auth.GetBlobClient(JobOpportunitiesContainerName, blobNameJobOpportunities, _logger, _config);
                var blobClientSkills = await Auth.GetBlobClient(JobOpportunitySkillsContainerName, blobNameSkills, _logger, _config);
                var blobClientJobType = await Auth.GetBlobClient(JobOpportunityJobTypesContainerName, blobNameJobTypes, _logger, _config);

                using var blobStreamJobOpp = await blobClientJobOpp.OpenWriteAsync(overwrite: true, cancellationToken: ct);
                using var blobStreamSkills = await blobClientSkills.OpenWriteAsync(overwrite: true, cancellationToken: ct);
                using var blobStreamJobType = await blobClientJobType.OpenWriteAsync(overwrite: true, cancellationToken: ct);

                // JobOpportunity fields
                var idField = new DataField<int>("Id");
                var viewCountField = new DataField<int>("ViewCount");
                var applyCountField = new DataField<int>("ApplyCount");
                var authorMailField = new DataField<string>("AuthorMail");
                var departmentIdField = new DataField<int>("DepartmentId");
                var departmentField = new DataField<string>("Department");
                var titleEnField = new DataField<string>("JobTitleEn");
                var classCodeIdField = new DataField<int>("ClassificationCodeId");
                var classCodeField = new DataField<string>("ClassificationCode");
                var classLevelIdField = new DataField<int>("ClassificationLevelId");
                var classLevelField = new DataField<string>("ClassificationLevel");
                var numOpportunitiesField = new DataField<int>("NumberOfOpportunities");
                var durationIdField = new DataField<int?>("DurationId");
                var durationField = new DataField<string?>("Duration");
                var durationQuantityField = new DataField<double>("DurationQuantity");
                var creationDateField = new DataField<DateTime>("CreationDate");
                var modificationDateField = new DataField<DateTime>("ModificationDate");
                var applicationDeadlineDateField = new DataField<DateTime>("ApplicationDeadlineDate");
                var workscheduleIdField = new DataField<int>("WorkScheduleId");
                var workscheduleField = new DataField<string>("WorkSchedule");
                var securityClearanceIdField = new DataField<int>("SecurityClearanceId");
                var securityClearanceField = new DataField<string>("SecurityClearance");
                var languageComprehensionField = new DataField<string?>("LanguageComprehension");
                var languageRequirementIdField = new DataField<int>("LanguageRequirementId");
                var languageRequirementField = new DataField<string>("LanguageRequirement");
                var workArrangementIdField = new DataField<int>("WorkArrangementId");
                var workArrangementField = new DataField<string>("WorkArrangement");
                var approvedStaffindField = new DataField<bool>("ApprovedStaffing");
                var cityIdField = new DataField<int?>("CityId");
                var cityField = new DataField<string?>("City");
                var regionIdField = new DataField<int?>("RegionId");
                var regionField = new DataField<string?>("Region");
                var provinceIdField = new DataField<int?>("ProvinceId");
                var provinceField = new DataField<string?>("Province");
                var programAreaIdField = new DataField<string?>("ProgramAreaId");
                var programAreaField = new DataField<string?>("ProgramArea");
                var snapshotDateField = new DataField<DateTime>("SnapshotDate");

                // Skill/JobType fields
                var jobOpportunityIdField = new DataField<int>("JobOpportunityId");
                var skillIdField = new DataField<int>("SkillId");
                var jobTypeIdField = new DataField<string>("JobTypeId");
                var titleField = new DataField<string>("Title");

                var schemaJobOpp = new ParquetSchema(idField, viewCountField, applyCountField, authorMailField, departmentIdField, departmentField, titleEnField, classCodeIdField, classCodeField, classLevelIdField,
                    classLevelField, numOpportunitiesField, durationIdField, durationField, durationQuantityField, creationDateField, modificationDateField, applicationDeadlineDateField,
                    workscheduleIdField, workscheduleField, securityClearanceIdField, securityClearanceField, languageComprehensionField, languageRequirementIdField, languageRequirementField,
                    workArrangementIdField, workArrangementField, approvedStaffindField, cityIdField, cityField, regionIdField, regionField, provinceIdField, provinceField, programAreaIdField,
                    programAreaField, snapshotDateField);
                await using var parquetWriterJobOpp = await ParquetWriter.CreateAsync(schemaJobOpp, blobStreamJobOpp, Globals.ParquetOptions, cancellationToken: ct);

                var schemaSkills = new ParquetSchema(jobOpportunityIdField, skillIdField, titleField, snapshotDateField);
                await using var parquetWriterSkills = await ParquetWriter.CreateAsync(schemaSkills, blobStreamSkills, Globals.ParquetOptions, cancellationToken: ct);

                var schemaJobType = new ParquetSchema(jobOpportunityIdField, jobTypeIdField, titleField, snapshotDateField);
                await using var parquetWriterJobType = await ParquetWriter.CreateAsync(schemaJobType, blobStreamJobType, Globals.ParquetOptions, cancellationToken: ct);

                // JobOpportunity buffers
                var idBuffer = new List<int>(Globals.RowGroupBatchSize);
                var viewCountBuffer = new List<int>(Globals.RowGroupBatchSize);
                var applyCountBuffer = new List<int>(Globals.RowGroupBatchSize);
                var authorMailBuffer = new List<string>(Globals.RowGroupBatchSize);
                var departmentIdBuffer = new List<int>(Globals.RowGroupBatchSize);
                var departmentBuffer = new List<string>(Globals.RowGroupBatchSize);
                var titleEnBuffer = new List<string>(Globals.RowGroupBatchSize);
                var classCodeIdBuffer = new List<int>(Globals.RowGroupBatchSize);
                var classCodeBuffer = new List<string>(Globals.RowGroupBatchSize);
                var classLevelIdBuffer = new List<int>(Globals.RowGroupBatchSize);
                var classLevelBuffer = new List<string>(Globals.RowGroupBatchSize);
                var numOpportunitiesBuffer = new List<int>(Globals.RowGroupBatchSize);
                var durationIdBuffer = new List<int?>(Globals.RowGroupBatchSize);
                var durationBuffer = new List<string?>(Globals.RowGroupBatchSize);
                var durationQuantityBuffer = new List<double>(Globals.RowGroupBatchSize);
                var creationDateBuffer = new List<DateTime>(Globals.RowGroupBatchSize);
                var modificationDateBuffer = new List<DateTime>(Globals.RowGroupBatchSize);
                var applicationDeadlineDateBuffer = new List<DateTime>(Globals.RowGroupBatchSize);
                var workscheduleIdBuffer = new List<int>(Globals.RowGroupBatchSize);
                var workscheduleBuffer = new List<string>(Globals.RowGroupBatchSize);
                var securityClearanceIdBuffer = new List<int>(Globals.RowGroupBatchSize);
                var securityClearanceBuffer = new List<string>(Globals.RowGroupBatchSize);
                var languageComprehensionBuffer = new List<string?>(Globals.RowGroupBatchSize);
                var languageRequirementIdBuffer = new List<int>(Globals.RowGroupBatchSize);
                var languageRequirementBuffer = new List<string>(Globals.RowGroupBatchSize);
                var workArrangementIdBuffer = new List<int>(Globals.RowGroupBatchSize);
                var workArrangementBuffer = new List<string>(Globals.RowGroupBatchSize);
                var approvedStaffindBuffer = new List<bool>(Globals.RowGroupBatchSize);
                var cityIdBuffer = new List<int?>(Globals.RowGroupBatchSize);
                var cityBuffer = new List<string?>(Globals.RowGroupBatchSize);
                var regionIdBuffer = new List<int?>(Globals.RowGroupBatchSize);
                var regionBuffer = new List<string?>(Globals.RowGroupBatchSize);
                var provinceIdBuffer = new List<int?>(Globals.RowGroupBatchSize);
                var provinceBuffer = new List<string?>(Globals.RowGroupBatchSize);
                var programAreaIdBuffer = new List<string?>(Globals.RowGroupBatchSize);
                var programAreaBuffer = new List<string?>(Globals.RowGroupBatchSize);
                var snapshotDateBuffer = new List<DateTime>(Globals.RowGroupBatchSize);

                async Task FlushJobOpportunityBatchAsync()
                {
                    ct.ThrowIfCancellationRequested();

                    if (idBuffer.Count == 0)
                        return;

                    using var groupWriter = parquetWriterJobOpp.CreateRowGroup();

                    await groupWriter.WriteAsync<int>(idField, idBuffer.ToArray().AsMemory(), cancellationToken: ct);
                    await groupWriter.WriteAsync<int>(viewCountField, viewCountBuffer.ToArray().AsMemory(), cancellationToken: ct);
                    await groupWriter.WriteAsync<int>(applyCountField, applyCountBuffer.ToArray().AsMemory(), cancellationToken: ct);
                    await groupWriter.WriteAsync(authorMailField, authorMailBuffer);
                    await groupWriter.WriteAsync<int>(departmentIdField, departmentIdBuffer.ToArray().AsMemory(), cancellationToken: ct);
                    await groupWriter.WriteAsync(departmentField, departmentBuffer);
                    await groupWriter.WriteAsync(titleEnField, titleEnBuffer);
                    await groupWriter.WriteAsync<int>(classCodeIdField, classCodeIdBuffer.ToArray().AsMemory(), cancellationToken: ct);
                    await groupWriter.WriteAsync(classCodeField, classCodeBuffer);
                    await groupWriter.WriteAsync<int>(classLevelIdField, classLevelIdBuffer.ToArray().AsMemory(), cancellationToken: ct);
                    await groupWriter.WriteAsync(classLevelField, classLevelBuffer);
                    await groupWriter.WriteAsync<int>(numOpportunitiesField, numOpportunitiesBuffer.ToArray().AsMemory(), cancellationToken: ct);
                    await groupWriter.WriteAsync<int>(durationIdField, durationIdBuffer.ToArray().AsMemory(), cancellationToken: ct);
                    await groupWriter.WriteAsync(durationField, durationBuffer);
                    await groupWriter.WriteAsync<double>(durationQuantityField, durationQuantityBuffer.ToArray().AsMemory(), cancellationToken: ct);
                    await groupWriter.WriteAsync<DateTime>(creationDateField, creationDateBuffer.ToArray().AsMemory(), cancellationToken: ct);
                    await groupWriter.WriteAsync<DateTime>(modificationDateField, modificationDateBuffer.ToArray().AsMemory(), cancellationToken: ct);
                    await groupWriter.WriteAsync<DateTime>(applicationDeadlineDateField, applicationDeadlineDateBuffer.ToArray().AsMemory(), cancellationToken: ct);
                    await groupWriter.WriteAsync<int>(workscheduleIdField, workscheduleIdBuffer.ToArray().AsMemory(), cancellationToken: ct);
                    await groupWriter.WriteAsync(workscheduleField, workscheduleBuffer);
                    await groupWriter.WriteAsync<int>(securityClearanceIdField, securityClearanceIdBuffer.ToArray().AsMemory(), cancellationToken: ct);
                    await groupWriter.WriteAsync(securityClearanceField, securityClearanceBuffer);
                    await groupWriter.WriteAsync(languageComprehensionField, languageComprehensionBuffer);
                    await groupWriter.WriteAsync<int>(languageRequirementIdField, languageRequirementIdBuffer.ToArray().AsMemory(), cancellationToken: ct);
                    await groupWriter.WriteAsync(languageRequirementField, languageRequirementBuffer);
                    await groupWriter.WriteAsync<int>(workArrangementIdField, workArrangementIdBuffer.ToArray().AsMemory(), cancellationToken: ct);
                    await groupWriter.WriteAsync(workArrangementField, workArrangementBuffer);
                    await groupWriter.WriteAsync<bool>(approvedStaffindField, approvedStaffindBuffer.ToArray().AsMemory(), cancellationToken: ct);
                    await groupWriter.WriteAsync<int>(cityIdField, cityIdBuffer.ToArray().AsMemory(), cancellationToken: ct);
                    await groupWriter.WriteAsync(cityField, cityBuffer);
                    await groupWriter.WriteAsync<int>(regionIdField, regionIdBuffer.ToArray().AsMemory(), cancellationToken: ct);
                    await groupWriter.WriteAsync(regionField, regionBuffer);
                    await groupWriter.WriteAsync<int>(provinceIdField, provinceIdBuffer.ToArray().AsMemory(), cancellationToken: ct);
                    await groupWriter.WriteAsync(provinceField, provinceBuffer);
                    await groupWriter.WriteAsync(programAreaIdField, programAreaIdBuffer);
                    await groupWriter.WriteAsync(programAreaField, programAreaBuffer);
                    await groupWriter.WriteAsync<DateTime>(snapshotDateField, snapshotDateBuffer.ToArray().AsMemory(), cancellationToken: ct);

                    idBuffer.Clear();
                    viewCountBuffer.Clear();
                    applyCountBuffer.Clear();
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
                var jobOpportunityIdSkillsBuffer = new List<int>(Globals.RowGroupBatchSize);
                var skillIdBuffer = new List<int>(Globals.RowGroupBatchSize);
                var titleSkillsBuffer = new List<string>(Globals.RowGroupBatchSize);
                var snapshotDateSkillsBuffer = new List<DateTime>(Globals.RowGroupBatchSize);

                async Task FlushSkillsBatchAsync()
                {
                    ct.ThrowIfCancellationRequested();

                    if (jobOpportunityIdSkillsBuffer.Count == 0)
                        return;

                    using var groupWriter = parquetWriterSkills.CreateRowGroup();

                    await groupWriter.WriteAsync<int>(jobOpportunityIdField, jobOpportunityIdSkillsBuffer.ToArray().AsMemory(), cancellationToken: ct);
                    await groupWriter.WriteAsync<int>(skillIdField, skillIdBuffer.ToArray().AsMemory(), cancellationToken: ct);
                    await groupWriter.WriteAsync(titleField, titleSkillsBuffer);
                    await groupWriter.WriteAsync<DateTime>(snapshotDateField, snapshotDateSkillsBuffer.ToArray().AsMemory(), cancellationToken: ct);

                    jobOpportunityIdSkillsBuffer.Clear();
                    skillIdBuffer.Clear();
                    titleSkillsBuffer.Clear();
                    snapshotDateSkillsBuffer.Clear();
                }

                // JobType buffers
                var jobOpportunityIdJobTypeBuffer = new List<int>(Globals.RowGroupBatchSize);
                var jobTypeIdBuffer = new List<string>(Globals.RowGroupBatchSize);
                var titleJobTypeBuffer = new List<string>(Globals.RowGroupBatchSize);
                var snapshotDateJobTypeBuffer = new List<DateTime>(Globals.RowGroupBatchSize);

                async Task FlushJobTypeBatchAsync()
                {
                    ct.ThrowIfCancellationRequested();

                    if (jobOpportunityIdJobTypeBuffer.Count == 0)
                        return;

                    using var groupWriter = parquetWriterJobType.CreateRowGroup();

                    await groupWriter.WriteAsync<int>(jobOpportunityIdField, jobOpportunityIdJobTypeBuffer.ToArray().AsMemory(), cancellationToken: ct);
                    await groupWriter.WriteAsync(jobTypeIdField, jobTypeIdBuffer);
                    await groupWriter.WriteAsync(titleField, titleJobTypeBuffer);
                    await groupWriter.WriteAsync<DateTime>(snapshotDateField, snapshotDateJobTypeBuffer.ToArray().AsMemory(), cancellationToken: ct);

                    jobOpportunityIdJobTypeBuffer.Clear();
                    jobTypeIdBuffer.Clear();
                    titleJobTypeBuffer.Clear();
                    snapshotDateJobTypeBuffer.Clear();
                }

                var graphClient = Auth.GetGraphServiceClient(_logger);

                var siteId = Auth.GetAppSetting("cmSiteId", _logger, _config);
                var listIdJobOpp = Auth.GetAppSetting("cmJobOpportunityListId", _logger, _config);

                // Cache the lookup lists
                var departments = await LoadLookupValuesAsync(graphClient, siteId, Auth.GetAppSetting("cmDepartmentListId", _logger, _config), ct: ct);
                var classificationCodes = await LoadLookupValuesAsync(graphClient, siteId, Auth.GetAppSetting("cmClassificationCodeListId", _logger, _config), ct: ct);
                var classificationLevels = await LoadLookupValuesAsync(graphClient, siteId, Auth.GetAppSetting("cmClassificationLevelListId", _logger, _config), ct: ct);
                var durations = await LoadLookupValuesAsync(graphClient, siteId, Auth.GetAppSetting("cmDurationListId", _logger, _config), ct: ct);
                var workSchedules = await LoadLookupValuesAsync(graphClient, siteId, Auth.GetAppSetting("cmWorkScheduleListId", _logger, _config), ct: ct);
                var securityClearances = await LoadLookupValuesAsync(graphClient, siteId, Auth.GetAppSetting("cmSecurityClearanceListId", _logger, _config), ct: ct);
                var languageRequirements = await LoadLookupValuesAsync(graphClient, siteId, Auth.GetAppSetting("cmLanguageRequirementListId", _logger, _config), ct: ct);
                var workArrangements = await LoadLookupValuesAsync(graphClient, siteId, Auth.GetAppSetting("cmWorkArrangementListId", _logger, _config), ct: ct);
                var cities = await LoadLookupValuesAsync(graphClient, siteId, Auth.GetAppSetting("cmCityListId", _logger, _config), new[] { "NameEn", "TitleEn", "RegionLookupId" }, ct: ct);
                var regions = await LoadLookupValuesAsync(graphClient, siteId, Auth.GetAppSetting("cmRegionListId", _logger, _config), new[] { "NameEn", "TitleEn", "ProvinceLookupId" }, ct: ct);
                var provinces = await LoadLookupValuesAsync(graphClient, siteId, Auth.GetAppSetting("cmProvinceListId", _logger, _config), new[] { "NameEn", "TitleEn" }, ct: ct);
                var skills = await LoadLookupValuesAsync(graphClient, siteId, Auth.GetAppSetting("cmSkillsListId", _logger, _config), ct: ct);
                var programAreaByItem = await LoadProgramAreasAsync(graphClient, siteId, listIdJobOpp, ProgramAreaColumnName, ct: ct);

                // Cache the Google Analytics events
                var viewEvents = await GetGoogleAnalyticsEvents($"GA4Report_453422813_{DateTime.UtcNow.ToString(Globals.BlobDateFormat)}_ViewJobOpportunity.json", "stats", ct: ct);
                var applyEvents = await GetGoogleAnalyticsEvents($"GA4Report_453422813_{DateTime.UtcNow.ToString(Globals.BlobDateFormat)}_ApplyJobOpportunity.json", "stats", ct: ct);

                // IMPORTANT: Confirm these are named the same for your environment
                var selectColumns = new[]
                {
                    "ContactEmail",
                    "DepartmentLookupId",
                    "JobTitleEn",
                    "ClassificationCodeLookupId",
                    "ClassificationLevelLookupId",
                    "NumberOfOpportunities",
                    "DurationLookupId",
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
                    "Skills",
                    "JobType",
                    ProgramAreaColumnName,
                };

                var jobOpportunityPage = await graphClient.Sites[siteId].Lists[listIdJobOpp].Items.GetAsync(rc =>
                {
                    rc.QueryParameters.Expand = new[] { $"fields($select={string.Join(",", selectColumns)})" };
                    //rc.QueryParameters.Expand = new[] { "fields" };
                }, ct);

                var countJobOpp = 0;
                var countSkills = 0;
                var countJobType = 0;
                
                var pageIterator = PageIterator<ListItem, ListItemCollectionResponse>
                    .CreatePageIterator(
                        graphClient,
                        jobOpportunityPage!,
                        jobOpportunity =>
                        {
                            ct.ThrowIfCancellationRequested();

                            var fields = jobOpportunity.Fields?.AdditionalData;
                            if (jobOpportunity.Id != null && fields != null)
                            {
                                idBuffer.Add(Convert.ToInt32(jobOpportunity.Id));
                                authorMailBuffer.Add(Globals.GetField(fields, "ContactEmail"));

                                var viewCount = viewEvents.FirstOrDefault(e => e.JobOpportunityId == jobOpportunity.Id)?.EventCount ?? 0;
                                viewCountBuffer.Add(viewCount);

                                var applyCount = applyEvents.FirstOrDefault(e => e.JobOpportunityId == jobOpportunity.Id)?.EventCount ?? 0;
                                applyCountBuffer.Add(applyCount);

                                var departmentId = Globals.GetField(fields, "DepartmentLookupId");
                                departmentIdBuffer.Add(Convert.ToInt32(departmentId));
                                departmentBuffer.Add(departments.Where(item => item.Id == departmentId).FirstOrDefault()?.Title!);

                                titleEnBuffer.Add(Globals.GetField(fields, "JobTitleEn"));

                                var classCodeId = Globals.GetField(fields, "ClassificationCodeLookupId");
                                classCodeIdBuffer.Add(Convert.ToInt32(classCodeId));
                                classCodeBuffer.Add(classificationCodes.Where(item => item.Id == classCodeId).FirstOrDefault()?.Title!);

                                var classLevelId = Globals.GetField(fields, "ClassificationLevelLookupId");
                                classLevelIdBuffer.Add(Convert.ToInt32(classLevelId));
                                classLevelBuffer.Add(classificationLevels.Where(item => item.Id == classLevelId).FirstOrDefault()?.Title!);

                                numOpportunitiesBuffer.Add(Convert.ToInt32(Convert.ToDouble(Globals.GetField(fields, "NumberOfOpportunities"))));

                                var durationId = Globals.GetFieldOrNull(fields, "DurationLookupId");
                                durationIdBuffer.Add(durationId != null ? Convert.ToInt32(durationId) : (int?)null);
                                durationBuffer.Add(durationId != null ? durations.Where(item => item.Id == durationId).FirstOrDefault()?.Title! : null);

                                durationQuantityBuffer.Add(Convert.ToDouble(Globals.GetField(fields, "DurationQuantity")));
                                creationDateBuffer.Add(Convert.ToDateTime(Globals.GetField(fields, "Created")));
                                modificationDateBuffer.Add(Convert.ToDateTime(Globals.GetField(fields, "Modified")));
                                applicationDeadlineDateBuffer.Add(Convert.ToDateTime(Globals.GetField(fields, "ApplicationDeadlineDate")));

                                var workScheduleId = Globals.GetField(fields, "WorkScheduleLookupId");
                                workscheduleIdBuffer.Add(Convert.ToInt32(workScheduleId));
                                workscheduleBuffer.Add(workSchedules.Where(item => item.Id == workScheduleId).FirstOrDefault()?.Title!);

                                var securityClearanceId = Globals.GetField(fields, "SecurityClearanceLookupId");
                                securityClearanceIdBuffer.Add(Convert.ToInt32(securityClearanceId));
                                securityClearanceBuffer.Add(securityClearances.Where(item => item.Id == securityClearanceId).FirstOrDefault()?.Title!);

                                languageComprehensionBuffer.Add(Globals.GetField(fields, "LanguageComprehension"));

                                var languageRequirementId = Globals.GetField(fields, "LanguageRequirementLookupId");
                                languageRequirementIdBuffer.Add(Convert.ToInt32(languageRequirementId));
                                languageRequirementBuffer.Add(languageRequirements.Where(item => item.Id == languageRequirementId).FirstOrDefault()?.Title!);

                                var workArrangementId = Globals.GetField(fields, "WorkArrangementLookupId");
                                workArrangementIdBuffer.Add(Convert.ToInt32(workArrangementId));
                                workArrangementBuffer.Add(workArrangements.Where(item => item.Id == workArrangementId).FirstOrDefault()?.Title!);

                                approvedStaffindBuffer.Add(Convert.ToBoolean(Globals.GetField(fields, "ApprovedStaffing")));

                                var cityId = Globals.GetFieldOrNull(fields, "CityLookupId");
                                cityIdBuffer.Add(cityId != null ? Convert.ToInt32(cityId) : (int?)null);
                                cityBuffer.Add(cityId != null ? cities.Where(item => item.Id == cityId).FirstOrDefault()?.Title : null);

                                var regionId = cityId != null ? cities.Where(city => city.Id == cityId).FirstOrDefault()?.ParentId : null;
                                regionIdBuffer.Add(regionId != null ? Convert.ToInt32(regionId) : (int?)null);
                                regionBuffer.Add(regionId != null ? regions.Where(item => item.Id == regionId).FirstOrDefault()?.Title : null);

                                var provinceId = regionId != null ? regions.Where(region => region.Id == regionId).FirstOrDefault()?.ParentId : null;
                                provinceIdBuffer.Add(provinceId != null ? Convert.ToInt32(provinceId) : (int?)null);
                                provinceBuffer.Add(provinceId != null ? provinces.Where(item => item.Id == provinceId).FirstOrDefault()?.Title : null);

                                programAreaByItem.TryGetValue(jobOpportunity.Id, out var pa);
                                programAreaIdBuffer.Add(pa.TermId);
                                programAreaBuffer.Add(pa.Label);

                                snapshotDateBuffer.Add(snapshotDate);

                                // Skills
                                var skillItems = new List<(string Id, string? Title)>();
                                if (fields.TryGetValue("Skills", out var rawSkills) && rawSkills != null)
                                {
                                    IEnumerable<UntypedNode> nodes = rawSkills switch
                                    {
                                        UntypedArray arr => arr.GetValue(),
                                        UntypedNode single => new[] { single },
                                        _ => Enumerable.Empty<UntypedNode>()
                                    };

                                    foreach (var node in nodes)
                                    {
                                        if (node is UntypedObject obj)
                                        {
                                            var dict = obj.GetValue();

                                            dict.TryGetValue("LookupId", out var lookupIdNode);
                                            dict.TryGetValue("LookupValue", out var lookupValueNode);

                                            var id = UntypedToString(lookupIdNode);
                                            if (id != null)
                                                skillItems.Add((id, UntypedToString(lookupValueNode)));
                                        }
                                        else
                                        {
                                            var id = UntypedToString(node);
                                            if (id != null)
                                                skillItems.Add((id, null));
                                        }
                                    }
                                }

                                foreach (var (skillId, lookupValue) in skillItems)
                                {
                                    jobOpportunityIdSkillsBuffer.Add(Convert.ToInt32(jobOpportunity.Id));
                                    skillIdBuffer.Add(Convert.ToInt32(skillId));
                                    titleSkillsBuffer.Add(skills.FirstOrDefault(s => s.Id == skillId)?.Title ?? lookupValue ?? string.Empty);
                                    snapshotDateSkillsBuffer.Add(snapshotDate);
                                    countSkills++;

                                    if (skillIdBuffer.Count >= Globals.RowGroupBatchSize)
                                        FlushSkillsBatchAsync().GetAwaiter().GetResult();
                                }

                                // Job Types
                                var jobTypeItems = new List<(string Id, string? Title)>();
                                if (fields.TryGetValue("JobType", out var rawJobTypes) && rawJobTypes != null)
                                {
                                    IEnumerable<UntypedNode> nodes = rawJobTypes switch
                                    {
                                        UntypedArray arr => arr.GetValue(),
                                        UntypedNode single => new[] { single },
                                        _ => Enumerable.Empty<UntypedNode>()
                                    };

                                    foreach (var node in nodes)
                                    {
                                        if (node is UntypedObject obj)
                                        {
                                            var dict = obj.GetValue();

                                            dict.TryGetValue("Label", out var lookupLabelNode);
                                            dict.TryGetValue("TermGuid", out var lookupGuidNode);

                                            var id = UntypedToString(lookupGuidNode);
                                            if (id != null)
                                                jobTypeItems.Add((id, UntypedToString(lookupLabelNode)));
                                        }
                                        else
                                        {
                                            var id = UntypedToString(node);
                                            if (id != null)
                                                jobTypeItems.Add((id, null));
                                        }
                                    }
                                }

                                foreach (var jobType in jobTypeItems)
                                {
                                    jobOpportunityIdJobTypeBuffer.Add(Convert.ToInt32(jobOpportunity.Id));
                                    jobTypeIdBuffer.Add(jobType.Id);
                                    titleJobTypeBuffer.Add(jobType.Title!);
                                    snapshotDateJobTypeBuffer.Add(snapshotDate);
                                    countJobType++;

                                    if (jobTypeIdBuffer.Count >= Globals.RowGroupBatchSize)
                                        FlushJobTypeBatchAsync().GetAwaiter().GetResult();
                                }

                                if (idBuffer.Count >= Globals.RowGroupBatchSize)
                                {
                                    FlushJobOpportunityBatchAsync().GetAwaiter().GetResult();
                                }

                                countJobOpp++;
                            }
                            return true;
                        });

                await pageIterator.IterateAsync(ct);

                await FlushJobOpportunityBatchAsync();
                await FlushSkillsBatchAsync();
                await FlushJobTypeBatchAsync();

                await parquetWriterJobOpp.DisposeAsync();
                await parquetWriterSkills.DisposeAsync();
                await parquetWriterJobType.DisposeAsync();

                _logger.LogInformation($"Streamed {countJobOpp} job opportunities to {blobNameJobOpportunities}");
                _logger.LogInformation($"Streamed {countSkills} skills to {blobNameSkills}");
                _logger.LogInformation($"Streamed {countJobType} job types to {blobNameJobTypes}");

                return blobNameJobOpportunities;
            }
            catch (OperationCanceledException)
            {
                _logger.LogWarning("StreamJobOpportunitiesToBlobAsync was cancelled");

                await Globals.DeletePartialBlob(JobOpportunitiesContainerName, blobNameJobOpportunities, _logger, _config);
                await Globals.DeletePartialBlob(JobOpportunitySkillsContainerName, blobNameSkills, _logger, _config);
                await Globals.DeletePartialBlob(JobOpportunityJobTypesContainerName, blobNameJobTypes, _logger, _config);

                throw;
            }
            catch (Exception ex)
            {
                _logger.LogError("StreamJobOpportunitiesToBlobAsync failed");
                _logger.LogError(ex.Message);

                await Globals.DeletePartialBlob(JobOpportunitiesContainerName, blobNameJobOpportunities, _logger, _config);
                await Globals.DeletePartialBlob(JobOpportunitySkillsContainerName, blobNameSkills, _logger, _config);
                await Globals.DeletePartialBlob(JobOpportunityJobTypesContainerName, blobNameJobTypes, _logger, _config);

                throw;
            }
        }

        private async Task<List<LookupValue>> LoadLookupValuesAsync(GraphServiceClient graphClient, string siteId, string listId, string[]? selectColumns = null, CancellationToken ct = default)
        {
            var values = new List<LookupValue>();
            
            var response = await graphClient.Sites[siteId].Lists[listId].Items.GetAsync(rc =>
            {
                rc.QueryParameters.Expand = selectColumns == null || selectColumns.Length == 0 ? ["fields"] : [$"fields($select={string.Join(",", selectColumns)})"];
            }, ct);

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

            await pageIterator.IterateAsync(ct);

            return values;
        }

        // For some reason the Program Area column is the only column that DOES NOT load when we get the items from the list... so we have to load them all first
        private async Task<ConcurrentDictionary<string, (string? TermId, string? Label)>> LoadProgramAreasAsync(
    GraphServiceClient graphClient, string siteId, string listId, string columnName, CancellationToken ct = default)
        {
            var result = new ConcurrentDictionary<string, (string?, string?)>();

            // 1. Collect item IDs with a lightweight call
            var ids = new List<string>();
            var page = await graphClient.Sites[siteId].Lists[listId].Items.GetAsync(rc =>
            {
                rc.QueryParameters.Select = new[] { "id" };
                rc.QueryParameters.Top = 999;
            }, ct);

            if (page == null) return result;

            var idIterator = PageIterator<ListItem, ListItemCollectionResponse>
                .CreatePageIterator(graphClient, page, item =>
                {
                    if (item.Id != null) ids.Add(item.Id);
                    return true;
                });
            await idIterator.IterateAsync(ct);

            // 2. Fetch each item's fields, a few at a time
            using var gate = new SemaphoreSlim(8);

            var tasks = ids.Select(async id =>
            {
                await gate.WaitAsync(ct);
                try
                {
                    var f = await graphClient.Sites[siteId].Lists[listId].Items[id].Fields.GetAsync(rc =>
                    {
                        rc.QueryParameters.Select = new[] { columnName };
                    }, ct);

                    if (f?.AdditionalData == null ||
                        !f.AdditionalData.TryGetValue(columnName, out var raw) || raw == null)
                        return;

                    UntypedNode? node = raw switch
                    {
                        UntypedArray arr => arr.GetValue().FirstOrDefault(),
                        UntypedNode n => n,
                        _ => null
                    };

                    if (node is UntypedObject obj)
                    {
                        var dict = obj.GetValue();
                        dict.TryGetValue("TermGuid", out var termNode);
                        dict.TryGetValue("Label", out var labelNode);
                        result[id] = (UntypedToString(termNode), UntypedToString(labelNode));
                    }
                }
                finally
                {
                    gate.Release();
                }
            });

            await Task.WhenAll(tasks);
            return result;
        }

        private async Task<List<GoogleAnalyticsEvent>> GetGoogleAnalyticsEvents(string blobName, string containerName, CancellationToken ct = default)
        {
            try
            {
                var storageAccountUrl = Auth.GetAppSetting("storageAccountUrlGA", _logger, _config);
                var isLocal = Auth.GetAppSetting("isLocal", _logger, _config, false);
                var credential = isLocal == "true" ? new AzureCliCredential() : (Azure.Core.TokenCredential)new DefaultAzureCredential();

                var blobServiceClient = new BlobServiceClient(new Uri(storageAccountUrl), credential);
                var containerClient = blobServiceClient.GetBlobContainerClient(containerName);
                var blobClient = containerClient.GetBlobClient(blobName);

                if (!await blobClient.ExistsAsync(ct))
                    throw new FileNotFoundException($"Blob {blobName} not found in container {containerName}");

                var response = await blobClient.DownloadContentAsync(ct);
                var json = response.Value.Content.ToString();

                var events = JsonConvert.DeserializeObject<List<GoogleAnalyticsEvent>>(json);

                return events ?? new List<GoogleAnalyticsEvent>();
            }
            catch (Exception ex)
            {
                _logger.LogError("GetGoogleAnalyticsReport failed");
                _logger.LogError(ex.Message);
                throw;
            }
        }

        private static string? UntypedToString(UntypedNode? node) => node switch
        {
            UntypedString s => s.GetValue(),
            UntypedInteger i => i.GetValue().ToString(CultureInfo.InvariantCulture),
            UntypedLong l => l.GetValue().ToString(CultureInfo.InvariantCulture),
            UntypedFloat f => f.GetValue().ToString("0", CultureInfo.InvariantCulture),
            UntypedDouble d => d.GetValue().ToString("0", CultureInfo.InvariantCulture),
            UntypedDecimal m => m.GetValue().ToString("0", CultureInfo.InvariantCulture),
            UntypedBoolean b => b.GetValue().ToString(),
            _ => null
        };
    }
}
