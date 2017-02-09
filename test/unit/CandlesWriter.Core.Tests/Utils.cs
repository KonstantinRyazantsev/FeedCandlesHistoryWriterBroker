using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace CandlesWriter.Core.Tests
{
    public static class Utils
    {
        public static DateTime ParseUtc(string dateTime)
        {
            return DateTimeOffset.Parse(dateTime).UtcDateTime;
        }
    }
}
