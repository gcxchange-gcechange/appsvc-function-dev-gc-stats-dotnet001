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
        public async Task Run([TimerTrigger(Globals.TimerStartTime)] TimerInfo timer)
        {
            _logger.LogInformation("TotalUsers timer trigger executed at: {Time}", DateTime.UtcNow);

            IConfiguration config = new ConfigurationBuilder()
                .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true)
                .AddEnvironmentVariables()
                .Build();

            await Users.StreamUsersToBlobAsync(_logger, config);

            if (timer.ScheduleStatus is not null)
            {
                _logger.LogInformation("Next scheduled run: {Next}", timer.ScheduleStatus.Next);
            }
        }
    }
}