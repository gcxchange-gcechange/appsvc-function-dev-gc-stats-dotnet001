using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
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
        public async Task<IActionResult> Run([HttpTrigger(AuthorizationLevel.Function, "get")] HttpRequest req)
        {
            _logger.LogInformation("C# HTTP trigger function processed a request.");

            IConfiguration config = new ConfigurationBuilder()
               .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true)
               .AddEnvironmentVariables()
               .Build();

            await Communities.StreamCommunitiesToBlobAsync(_logger, config);

            return new OkObjectResult("Welcome to Azure Functions!");
        }
    }
}
