using Azure.Identity;
using Azure.Storage.Queues;
using GCStats;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Builder;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

var builder = FunctionsApplication.CreateBuilder(args);

builder.ConfigureFunctionsWebApplication();

builder.Services.AddHttpClient();

builder.Services
    .AddApplicationInsightsTelemetryWorkerService()
    .ConfigureFunctionsApplicationInsights();

// Queue client shared by every job
builder.Services.AddSingleton(sp =>
{
    var cfg = sp.GetRequiredService<IConfiguration>();

    var options = new QueueClientOptions { MessageEncoding = QueueMessageEncoding.Base64 };

    var queueUri = cfg["AzureWebJobsStorage:queueServiceUri"];
    if (!string.IsNullOrEmpty(queueUri))
        return new QueueServiceClient(new Uri(queueUri), new DefaultAzureCredential(), options);

    var accountName = cfg["AzureWebJobsStorage:accountName"];
    if (!string.IsNullOrEmpty(accountName))
        return new QueueServiceClient(
            new Uri($"https://{accountName}.queue.core.windows.net"),
            new DefaultAzureCredential(), options);

    return new QueueServiceClient(cfg["AzureWebJobsStorage"], options);
});

builder.Services.AddSingleton<JobQueue>();

// Add the jobs here
builder.Services.AddSingleton<IJob, ActiveUsers>();
builder.Services.AddSingleton<IJob, TotalUsers>();
builder.Services.AddSingleton<IJob, TotalCommunities>();
builder.Services.AddSingleton<IJob, TotalFiles>();
builder.Services.AddSingleton<IJob, PageViews>();
builder.Services.AddSingleton<IJob, JobOpportunities>();

builder.Build().Run();