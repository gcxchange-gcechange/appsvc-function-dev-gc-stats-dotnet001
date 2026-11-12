using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Kiota.Abstractions.Serialization;
using Parquet;
using System.Text.Json;

namespace GCStats
{
    static class Globals
    {
        public const string TimerStartTime = "0 0 7 * * *"; // 7 AM UTC = 2 AM EST
        public const string BlobDateFormat = "yyyy-MM-dd";
        public const int RowGroupBatchSize = 50000;

        public static readonly ParquetOptions ParquetOptions = new ParquetOptions
        {
            CompressionMethod = CompressionMethod.Snappy
        };

        public static string GetField(IDictionary<string, object> fields, string name)
        {
            if (fields is null) return null;

            // key casing varies, so match case-insensitively
            var key = fields.Keys.FirstOrDefault(k => string.Equals(k, name, StringComparison.OrdinalIgnoreCase));
            if (key is null) return null;

            return fields[key] switch
            {
                UntypedString s => s.GetValue(),
                UntypedInteger i => i.GetValue().ToString(),
                UntypedLong l => l.GetValue().ToString(),
                JsonElement je => je.ToString(),
                var v => v?.ToString()
            };
        }

        public static string? GetFieldOrNull(IDictionary<string, object> fields, string name)
        {
            var value = GetField(fields, name);
            return value == "0" ? null : value;
        }

        public static async Task DeletePartialBlob(string containerName, string blobName, ILogger log, IConfiguration config)
        {
            try
            {
                var client = await Auth.GetBlobClient(containerName, blobName, log, config);
                var deleted = await client.DeleteIfExistsAsync();

                if (deleted.Value)
                    log.LogWarning("Deleted partial blob {BlobName} from {Container}", blobName, containerName);
            }
            catch (Exception ex)
            {
                log.LogWarning(ex, "Could not delete partial blob {BlobName} from {Container}", blobName, containerName);
            }
        }
    }
}
