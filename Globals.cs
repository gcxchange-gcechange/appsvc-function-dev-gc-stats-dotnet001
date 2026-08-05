using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System.Text.Json;

namespace GCStats
{
    static class Globals
    {
        public const string TimerStartTime = "0 0 7 * * *"; // 7 AM UTC = 2 AM EST

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
    }
}
