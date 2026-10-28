using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;

namespace GCStats
{
    public class JobRunner
    {
        private readonly Dictionary<string, IJob> _jobs;
        private readonly ILogger<JobRunner> _logger;

        public JobRunner(IEnumerable<IJob> jobs, ILogger<JobRunner> logger)
        {
            _jobs = jobs.ToDictionary(j => j.Name);
            _logger = logger;
        }

        [Function("JobRunner")]
        public async Task Run([QueueTrigger(JobQueue.QueueName, Connection = "AzureWebJobsStorage")] string jobName, FunctionContext ctx)
        {
            if (!_jobs.TryGetValue(jobName, out var job))
            {
                _logger.LogWarning($"Unknown job {jobName}");
                return;
            }

            _logger.LogInformation($"Starting {jobName}");

            await job.RunAsync(ctx.CancellationToken);

            _logger.LogInformation($"Finished {jobName}");
        }
    }

    public interface IJob
    {
        string Name { get; }
        Task RunAsync(CancellationToken ct);
    }
}
