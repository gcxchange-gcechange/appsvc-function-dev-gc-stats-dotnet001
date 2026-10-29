using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace GCStats
{
    public class TotalCommunities : IJob
    {
        private readonly ILogger<TotalCommunities> _logger;
        private readonly IConfiguration _config;
        private readonly JobQueue _queue;

        public string Name => "TotalCommunities";

        public TotalCommunities(ILogger<TotalCommunities> logger, IConfiguration config, JobQueue queue)
        {
            _logger = logger;
            _config = config;
            _queue = queue;
        }

        [Function("Timer_TotalCommunities")]
        public Task Schedule([TimerTrigger(Globals.TimerStartTime)] TimerInfo timer) => _queue.EnqueueJobAsync(Name);

        public async Task RunAsync(CancellationToken ct)
        {
            _logger.LogInformation($"{Name} executed at: {DateTime.UtcNow}");

            var blobName = await Communities.StreamCommunitiesToBlobAsync(_logger, _config);

            _logger.LogInformation($"BlobName: {blobName}");

            await _queue.SendAsync("process-total-communities", blobName);
        }
    }
}
