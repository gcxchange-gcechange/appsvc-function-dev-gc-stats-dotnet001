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
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Net.Http.Headers;
using System.Reflection.Metadata;
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

        public async Task<string> Run([HttpTrigger(AuthorizationLevel.Function, "get", "post")] HttpRequest req, List<JsonElement> tickets)
        {

            _logger.LogInformation("Running Support Tickets");

            int page = 1;
            int perPage = 100;
           

            var snapshotDate = DateTime.UtcNow.Date;

            HttpClient client = _httpClientFactory.CreateClient();


            //get the key n domain
            var domain = Auth.GetAppSetting("freshDeskDomain", _logger, _config);
            var apiKey = Auth.GetAppSetting("freshDesk_API_KEY", _logger, _config);

            //convert key 
            string auth = Convert.ToBase64String(Encoding.UTF8.GetBytes($"{apiKey}:X"));

            client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic", auth);
            client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));

            // Define the fields for the parquet file
            var idField = new DataField<int>("Id");
            var productIdField = new DataField<int?>("ProductId");
            var subjectField = new DataField<string?>("Subject");
            var statusField = new DataField<string?>("Status");
            var createdAtField = new DataField<DateTime>("CreatedAt");
            var resolutionCodeField = new DataField<string>("ResolutionCode");
            var subTypesField = new DataField<string>("SubTypes");
            var snapshotDateField = new DataField<DateTime>("SnapshotDate");

            // Create the schema
            var ticketsSchema = new ParquetSchema(idField, productIdField, subjectField, statusField,  createdAtField, resolutionCodeField, subTypesField, snapshotDateField);

            //Create the buffers (rows)

            var idFieldBuffer = new List<int?>(Globals.RowGroupBatchSize);
            var productIdFieldBuffer = new List<int?>(Globals.RowGroupBatchSize);
            var subjectFieldBuffer = new List<string?>(Globals.RowGroupBatchSize);
            var statusFieldBuffer = new List<string?>(Globals.RowGroupBatchSize);
            var createdAtFieldBuffer = new List<DateTime?>(Globals.RowGroupBatchSize);
            var resolutionCodeFieldBuffer = new List<string>(Globals.RowGroupBatchSize);
            var subTypesFieldBuffer = new List<string?>(Globals.RowGroupBatchSize);
            var snapshotDateBuffer = new List<DateTime>(Globals.RowGroupBatchSize);

            while (true)
            {
                string url = $"{domain}per_page={perPage}&page={page}";
                //string url = $"{domain} ";


                //Get the data
                HttpResponseMessage response = await client.GetAsync(url);

                string responseBody = await response.Content.ReadAsStringAsync();
                response.EnsureSuccessStatusCode();

                using JsonDocument doc = JsonDocument.Parse(responseBody);
                int count = doc.RootElement.GetArrayLength();

                _logger.LogInformation($"Response: {doc.RootElement}");
                _logger.LogInformation($"Page {page}: {count} tickets");

                // Stop when there are no more tickets
                if (count == 0)
                    break;

                //Populate buffers from the API Response
                foreach (JsonElement item in doc.RootElement.EnumerateArray())
                {

                    {
                        idFieldBuffer.Add(item.GetProperty("id").GetInt32());
                        productIdFieldBuffer.Add(item.GetProperty("product_id").ValueKind == JsonValueKind.Null ? null : item.GetProperty("product_id").GetInt32());
                        subjectFieldBuffer.Add(item.GetProperty("subject").ValueKind == JsonValueKind.Null ? null : item.GetProperty("subject").ToString());
                        statusFieldBuffer.Add(item.GetProperty("status").ValueKind == JsonValueKind.Null ? null : item.GetProperty("status").ToString());
                        createdAtFieldBuffer.Add(DateTime.Parse(item.GetProperty("created_at").GetString()!));
                        resolutionCodeFieldBuffer.Add(item.GetProperty("custom_fields").GetProperty("cf_resolution_code").ToString());
                        subTypesFieldBuffer.Add(item.GetProperty("custom_fields").GetProperty("cf_sub_types").ToString());
                        snapshotDateBuffer.Add(snapshotDate);
                    }

                }


                page++;
            }


         
            var blobName = $"{TicketsContainerName}-{DateTime.UtcNow.ToString(Globals.BlobDateFormat)}.parquet";

            var blobClient = await Auth.GetBlobClient(TicketsContainerName, blobName, _logger, _config);


            using var blobStream = await blobClient.OpenWriteAsync(overwrite: true);

            await using var supporTicketsWriter = await ParquetWriter.CreateAsync(ticketsSchema, blobStream, Globals.ParquetOptions);

            using var ticketWriter = supporTicketsWriter.CreateRowGroup();

            await ticketWriter.WriteAsync<int>(idField, idFieldBuffer.ToArray());
            await ticketWriter.WriteAsync<int>(productIdField, productIdFieldBuffer.ToArray());
            await ticketWriter.WriteAsync(subjectField, subjectFieldBuffer);
            await ticketWriter.WriteAsync(statusField, statusFieldBuffer);
            await ticketWriter.WriteAsync<DateTime>(createdAtField, createdAtFieldBuffer.ToArray().AsMemory());
            await ticketWriter.WriteAsync(resolutionCodeField, resolutionCodeFieldBuffer);
            await ticketWriter.WriteAsync(subTypesField, subTypesFieldBuffer);
            await ticketWriter.WriteAsync<DateTime>(snapshotDateField, snapshotDateBuffer.ToArray().AsMemory());

            return blobName;


        }     
    }
}
