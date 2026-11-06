using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace GCStats
{
    public class TotalUsers : IJob
    {
        private readonly ILogger<TotalUsers> _logger;
        private readonly IConfiguration _config;
        private readonly JobQueue _queue;

        public string Name => "TotalUsers";

        public TotalUsers(ILogger<TotalUsers> logger, IConfiguration config, JobQueue queue)
        {
            _logger = logger;
            _config = config;
            _queue = queue;
        }

        [Function("Timer_TotalUsers")]
        public Task Schedule([TimerTrigger(Globals.TimerStartTime)] TimerInfo timer) => _queue.EnqueueJobAsync(Name);

        public async Task RunAsync(CancellationToken ct)
        {
            _logger.LogInformation($"{Name} executed at: {DateTime.UtcNow}");

            var blobName = await Users.StreamUsersToBlobAsync(_logger, _config, ct);

            _logger.LogInformation($"BlobName: {blobName}");

            await _queue.SendAsync("process-total-users", blobName);
        }
    }
}