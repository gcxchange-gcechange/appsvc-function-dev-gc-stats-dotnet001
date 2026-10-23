using Azure.Storage.Blobs;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Parquet;
using Parquet.Schema;
using System;
using System.Collections.Generic;
using System.Net.Http.Headers;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using System.Xml.Linq;

namespace GCStats
{
    public class SupportTickets
    {
        private readonly ILogger<SupportTickets> _logger;
        private readonly IConfiguration _config;
        private readonly IHttpClientFactory _httpClientFactory;

        public const string TicketsContainerName = "tickets";

        public SupportTickets(ILogger<SupportTickets> logger, IConfiguration config, IHttpClientFactory httpClientFactory)
        {
            _logger = logger;
            _config = config;
            _httpClientFactory = httpClientFactory;


        }

        [Function("SupportTickets")]
        [QueueOutput("process-support-tickets", Connection = "AzureWebJobsStorage")]

        public async Task<string> Run([HttpTrigger(AuthorizationLevel.Function, "get", "post")] HttpRequest req)
        {

            _logger.LogInformation("Running Support Tickets");

            HttpClient client = _httpClientFactory.CreateClient();

            //get the key n domain
            var domain = Auth.GetAppSetting("freshDeskDomain", _logger, _config);
            var apiKey = Auth.GetAppSetting("freshDesk_API_KEY", _logger, _config);

            //convert key 
            string auth = Convert.ToBase64String(Encoding.UTF8.GetBytes($"{apiKey}:X"));

           
            client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic", auth);
            client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
            //string query = "\"created_at:'2026-09-01'\"";

            string url = $"{domain}";
            //string url = $"{domain}" + $"?query={Uri.EscapeDataString(query)} ";

            HttpResponseMessage response = await client.GetAsync(url);

            string responseBody = await response.Content.ReadAsStringAsync();
            response.EnsureSuccessStatusCode();

            _logger.LogInformation($"HTTP Status: {(int)response.StatusCode}");

            using JsonDocument doc = JsonDocument.Parse(responseBody);

            var snapshotDate = DateTime.UtcNow.Date;

            // Define the fields for the parquet file
            var idField = new DataField<string>("Id");
            var productIdField = new DataField<string?>("ProductId");
            var subjectField = new DataField<string?>("Subject");
            var createdAtField = new DataField<string?>("CreatedAt");
            var snapshotDateField = new DataField<DateTime>("SnapshotDate");

            // Create the schema
            var ticketsSchema = new ParquetSchema(idField, productIdField, subjectField, createdAtField, snapshotDateField);

            //Create the buffers (rows)

            var idFieldBuffer = new List<string>(Globals.RowGroupBatchSize);
            var productIdFieldBuffer = new List<string?>(Globals.RowGroupBatchSize);
            var subjectFieldBuffer = new List<string?>(Globals.RowGroupBatchSize);
            var createdAtFieldBuffer = new List<string?>(Globals.RowGroupBatchSize);
            var snapshotDateBuffer = new List<DateTime>(Globals.RowGroupBatchSize);

            //Populate buffers from the API Response
            foreach (JsonElement item in doc.RootElement.EnumerateArray())
            {

                {
                    idFieldBuffer.Add(item.GetProperty("id").ToString());
                    productIdFieldBuffer.Add(item.GetProperty("product_id").ValueKind == JsonValueKind.Null ? null : item.GetProperty("product_id").ToString());
                    subjectFieldBuffer.Add(item.GetProperty("subject").ValueKind == JsonValueKind.Null ? null : item.GetProperty("subject").ToString());
                    createdAtFieldBuffer.Add(item.GetProperty("created_at").ToString());
                    snapshotDateBuffer.Add(snapshotDate);
                }

            }

          

            var blobName = $"{TicketsContainerName}-{DateTime.UtcNow.ToString(Globals.BlobDateFormat)}.parquet";

            var blobClient = await Auth.GetBlobClient(TicketsContainerName, blobName, _logger, _config);


            using var blobStream = await blobClient.OpenWriteAsync(overwrite: true);

            await using var supporTicketsWriter = await ParquetWriter.CreateAsync(ticketsSchema, blobStream, Globals.ParquetOptions);

            using var ticketWriter = supporTicketsWriter.CreateRowGroup();

            await ticketWriter.WriteAsync(idField, idFieldBuffer);
            await ticketWriter.WriteAsync(productIdField, productIdFieldBuffer);
            await ticketWriter.WriteAsync(subjectField, subjectFieldBuffer);
            await ticketWriter.WriteAsync(createdAtField, createdAtFieldBuffer);
            await ticketWriter.WriteAsync<DateTime>(snapshotDateField, snapshotDateBuffer.ToArray().AsMemory());

            return blobName;


        }
    }
}
