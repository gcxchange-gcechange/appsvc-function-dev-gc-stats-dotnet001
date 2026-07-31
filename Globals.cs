using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

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
    }
}
