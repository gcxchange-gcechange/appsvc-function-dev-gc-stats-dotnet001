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
    }
}
