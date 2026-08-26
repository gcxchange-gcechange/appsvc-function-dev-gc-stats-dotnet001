using Microsoft.AspNetCore.Http;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace GCStats
{
    public class TotalUsers
    {
        private readonly ILogger<TotalUsers> _logger;

        public TotalUsers(ILogger<TotalUsers> logger)
        {
            _logger = logger;
        }

        [Function("TotalUsers")]
        [QueueOutput("process-total-users", Connection = "AzureWebJobsStorage")]
        public async Task<string> Run([TimerTrigger(Globals.TimerStartTime)] TimerInfo timer)
        //[Function("TotalUsers")]
        //[QueueOutput("process-total-users", Connection = "AzureWebJobsStorage")]
        //public async Task<string> Run([HttpTrigger(AuthorizationLevel.Function, "get", "post")] HttpRequest req)
        {
            _logger.LogInformation("TotalUsers timer trigger executed at: {Time}", DateTime.UtcNow);

            IConfiguration config = new ConfigurationBuilder()
                .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true)
                .AddEnvironmentVariables()
                .Build();

            var blobName = await Users.StreamUsersToBlobAsync(_logger, config);

            _logger.LogInformation($"BlobName: {blobName}");

            return blobName;
        }
    }
}