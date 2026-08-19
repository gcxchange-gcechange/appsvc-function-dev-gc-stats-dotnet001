using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;

namespace GCStats
{
    public class ProcessTotalUsers
    {
        private readonly ILogger<ProcessTotalUsers> _logger;

        public ProcessTotalUsers(ILogger<ProcessTotalUsers> logger)
        {
            _logger = logger;
        }

        [Function("ProcessTotalUsers")]
        public async Task Run([QueueTrigger("process-total-users", Connection = "AzureWebJobsStorage")] string message)
        {
            _logger.LogInformation("Received queue message: {Message}", message);

            // Move data from storage into data warehouse

            _logger.LogInformation("Finished processing {Message}", message);
        }
    }
}
