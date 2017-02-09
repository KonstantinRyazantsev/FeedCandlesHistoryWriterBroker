using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Microsoft.WindowsAzure.Storage.Table;

namespace CandlesWriter.Core.IntTests.Legacy
{
    public class FeedCandle
    {
        [JsonProperty("O")]
        public double Open { get; internal set; }
        [JsonProperty("C")]
        public double Close { get; internal set; }
        [JsonProperty("H")]
        public double High { get; internal set; }
        [JsonProperty("L")]
        public double Low { get; internal set; }
        [JsonProperty("T")]
        public int Time { get; set; }
    }

    public class FeedCandleEntity : TableEntity
    {
        public string Data { get; set; }
        public DateTime DateTime { get; set; }
        public double Open { get; set; }
        public double Close { get; set; }
        public double High { get; set; }
        public double Low { get; set; }
        public bool IsBuy { get; set; }
        public int Time { get; set; }
    }
}
