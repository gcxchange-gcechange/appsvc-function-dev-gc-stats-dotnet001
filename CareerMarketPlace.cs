using Microsoft.AspNetCore.Http;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Graph;
using Microsoft.Graph.Models;
using Parquet.Schema;

namespace GCStats
{
    public class CareerMarketPlace
    {
        private readonly ILogger<CareerMarketPlace> _logger;
        private readonly IConfiguration _config;

        public const string CareerMarketplaceContainerName = "career-marketplace";

        public CareerMarketPlace(ILogger<CareerMarketPlace> logger, IConfiguration config)
        {
            _logger = logger;
            _config = config;
        }

        [Function("CareerMarketPlace")]
        [QueueOutput("process-career-marketplace", Connection = "AzureWebJobsStorage")]
        public async Task<string> Run([HttpTrigger(AuthorizationLevel.Function, "get", "post")] HttpRequest req)
        //public async Task<string> Run([TimerTrigger(Globals.TimerStartTime)] TimerInfo timer)
        {
            _logger.LogInformation("CareerMarketPlace timer trigger executed at: {Time}", DateTime.UtcNow);

            var blobName = await StreamJobOpportunitiesToBlobAsync();

            return blobName;
        }

        public async Task<string> StreamJobOpportunitiesToBlobAsync()
        {
            try
            {
                var snapshotDate = DateTime.UtcNow.Date;
                var blobName = $"{CareerMarketplaceContainerName}-{DateTime.UtcNow.ToString(Globals.BlobDateFormat)}.parquet";

                var graphClient = Auth.GetGraphServiceClient(_logger);
                var blobClient = await Auth.GetBlobClient(CareerMarketplaceContainerName, blobName, _logger, _config);

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
                var authorMail = new DataField<string>("AuthorMail");
                var departmentIdField = new DataField<string>("DepartmentId");
                var titleEnField = new DataField<string>("JobTitleEn");
                var titleFrField = new DataField<string>("JobTitleFr");
                var classCodeIdField = new DataField<string>("ClassificationCodeId");
                var classLevelIdField = new DataField<string>("ClassificationLevelId");
                var numOpportunitiesField = new DataField<string>("NumberOfOpportunities");
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
                var skillIdsField = new DataField<string>("SkillIds");      // Break into own Parquet file (JobOpportunitySkills)
                var cityIdField = new DataField<string>("CityId");
                var programAreaIdField = new DataField<string>("ProgramAreaId");
                var jobTypeIdsField = new DataField<string>("JobTypeIds");   // Break into own Parquet file (JobOpportunityJobTypes)

                var siteId = Auth.GetAppSetting("cmSiteId", _logger, _config);
                var listId = Auth.GetAppSetting("cmListId", _logger, _config);

                var jobOpportunityPage = await graphClient.Sites[siteId].Lists[listId].Items.GetAsync(rc =>
                {
                    rc.QueryParameters.Expand = new[] { "fields" };
                });

                var cols = await graphClient.Sites[siteId].Lists[listId].Columns.GetAsync();

                var lookups = cols!.Value!
                    .Where(c => c.Lookup != null && c.Hidden != true)
                    .Select(c => new { c.Name, c.Lookup!.ListId, c.Lookup.ColumnName });

                foreach (var l in lookups)
                    Console.WriteLine($"{l.Name} -> list {l.ListId}, column {l.ColumnName}");


                var pageIterator = PageIterator<ListItem, ListItemCollectionResponse>
                    .CreatePageIterator(
                        graphClient,
                        jobOpportunityPage!,
                        jobOpportunity =>
                        {
                            var f = jobOpportunity.Fields?.AdditionalData;
                            if (jobOpportunity.Id != null && f != null)
                            {
                                // TODO: Add to buffers
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
