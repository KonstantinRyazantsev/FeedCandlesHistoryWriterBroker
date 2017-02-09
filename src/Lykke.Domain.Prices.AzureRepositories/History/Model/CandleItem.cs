using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Lykke.Domain.Prices.Contracts;

namespace Lykke.Domain.Prices.AzureProvider.History.Model
{
    public class CandleItem //: ICandle
    {
        //[JsonIgnore]
        //public DateTime DateTime { get; internal set; }

        [JsonProperty("O")]
        public double Open { get; internal set; }

        [JsonProperty("C")]
        public double Close { get; internal set; }

        [JsonProperty("H")]
        public double High { get; internal set; }

        [JsonProperty("L")]
        public double Low { get; internal set; }

        //[JsonIgnore]
        //public bool IsBuy { get; internal set; }

        //[JsonIgnore]
        //public List<FeedCandle> Candles { get; set; } = new List<FeedCandle>();

        [JsonProperty("T")]
        public int Tick { get; set; }
    }
}
