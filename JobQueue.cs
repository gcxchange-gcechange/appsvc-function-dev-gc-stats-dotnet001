using Azure.Storage.Queues;

namespace GCStats
{
    public class JobQueue
    {
        public const string QueueName = "timer-jobs";
        private readonly QueueServiceClient _svc;

        public JobQueue(QueueServiceClient svc)
        {
            _svc = svc;
        }

        public async Task EnqueueJobAsync(string jobName)
        {
            var q = _svc.GetQueueClient(QueueName);
            await q.CreateIfNotExistsAsync();

            var waiting = await q.PeekMessagesAsync(maxMessages: 32);

            if (waiting.Value.Any(m => m.Body.ToString() == jobName)) 
                return;

            await q.SendMessageAsync(jobName);
        }

        public async Task SendAsync(string queueName, string message)
        {
            var q = _svc.GetQueueClient(queueName);
            await q.CreateIfNotExistsAsync();
            await q.SendMessageAsync(message);
        }
    }
}
