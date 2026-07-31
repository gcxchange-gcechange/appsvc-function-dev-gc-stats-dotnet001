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
        public async Task Run([TimerTrigger(Globals.TimerStartTime)] TimerInfo timer)
        {
            _logger.LogInformation("TotalCommunities timer trigger executed at: {Time}", DateTime.UtcNow);

            IConfiguration config = new ConfigurationBuilder()
               .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true)
               .AddEnvironmentVariables()
               .Build();

            await Communities.StreamCommunitiesToBlobAsync(_logger, config);

            if (timer.ScheduleStatus is not null)
            {
                _logger.LogInformation("Next scheduled run: {Next}", timer.ScheduleStatus.Next);
            }
        }
    }
}
