using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;

namespace GCStats
{
    public class ProcessTotalCommunities
    {
        private readonly ILogger<ProcessTotalCommunities> _logger;

        public ProcessTotalCommunities(ILogger<ProcessTotalCommunities> logger)
        {
            _logger = logger;
        }

        [Function("ProcessTotalCommunities")]
        public async Task Run([QueueTrigger("process-total-communities", Connection = "AzureWebJobsStorage")] string message)
        {
            _logger.LogInformation("Received queue message: {Message}", message);

            // Move data from storage into data warehouse

            _logger.LogInformation("Finished processing {Message}", message);
        }
    }
}