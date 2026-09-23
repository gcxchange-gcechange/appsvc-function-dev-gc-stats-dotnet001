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
    }
}
