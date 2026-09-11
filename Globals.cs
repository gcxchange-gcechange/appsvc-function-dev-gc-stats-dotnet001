using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System.Text.Json;

namespace GCStats
{
    static class Globals
    {
        public const string TimerStartTime = "0 0 7 * * *"; // 7 AM UTC = 2 AM EST
        public const string BlobDateFormat = "yyyy-MM-dd";

        public static readonly JsonSerializerOptions JsonOptions = new()
        {
            WriteIndented = false,
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase
        };

        public static string GetAppSetting(string settingName, ILogger log, IConfiguration config, bool isMandatory = true)
        {
            var value = config[settingName];

            if (value == null && isMandatory)
            {
                var msg = $"{settingName} is missing from the environment variables or local.settings.json";
                log.LogError(msg);
                throw new MissingFieldException(msg);
            }

            return value ?? string.Empty;
        }

        public static DateTime GetDateFromBlob(string blobName, ILogger log)
        {
            // TODO: Switch to parquet only when refactor is done.
            var matchJSON = System.Text.RegularExpressions.Regex.Match(blobName, @"(\d{4}-\d{2}-\d{2})\.json$");
            var matchParquet = System.Text.RegularExpressions.Regex.Match(blobName, @"(\d{4}-\d{2}-\d{2})\.parquet$");
            if (!matchJSON.Success && !matchParquet.Success)
            {
                throw new FormatException($"Could not extract date from blob name: {blobName}");
            }

            var snapshotDate = DateTime.ParseExact(matchJSON.Success ? matchJSON.Groups[1].Value : matchParquet.Groups[1].Value, BlobDateFormat, System.Globalization.CultureInfo.InvariantCulture);

            return snapshotDate;
        }
    }
}
