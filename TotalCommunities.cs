using Microsoft.AspNetCore.Http;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace GCStats
{
    public class TotalCommunities
    {
        private readonly ILogger<TotalCommunities> _logger;

        public TotalCommunities(ILogger<TotalCommunities> logger)
        {
            _logger = logger;
        }

        [Function("TotalCommunities")]
        [QueueOutput("process-total-communities", Connection = "AzureWebJobsStorage")]
        public async Task<string> Run([TimerTrigger(Globals.TimerStartTime)] TimerInfo timer)
        //[Function("TotalCommunities")]
        //[QueueOutput("process-total-communities", Connection = "AzureWebJobsStorage")]
        //public async Task<string> Run([HttpTrigger(AuthorizationLevel.Function, "get", "post")] HttpRequest req)
        {
            _logger.LogInformation("TotalCommunities timer trigger executed at: {Time}", DateTime.UtcNow);

            IConfiguration config = new ConfigurationBuilder()
               .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true)
               .AddEnvironmentVariables()
               .Build();

            var blobName = await Communities.StreamCommunitiesToBlobAsync(_logger, config);

            _logger.LogInformation($"BlobName: {blobName}");

            return blobName;
        }
    }
}
