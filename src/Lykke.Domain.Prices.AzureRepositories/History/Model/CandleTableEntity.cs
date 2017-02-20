using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using Newtonsoft.Json;

namespace Lykke.Domain.Prices.AzureProvider.History.Model
{
    public class CandleTableEntity : ITableEntity
    {
        public CandleTableEntity()
        {
        }

        public CandleTableEntity(string partitionKey, string rowKey)
        {
            this.PartitionKey = partitionKey;
            this.RowKey = rowKey;
        }

        #region ITableEntity properties

        public string ETag { get; set; }
        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
        public DateTimeOffset Timestamp { get; set; }

        #endregion

        public DateTime DateTime
        {
            get
            {
                // extract from RowKey + Interval from PKey
                if (!string.IsNullOrEmpty(this.RowKey))
                {
                    return ParseDateTime(this.RowKey, DateTimeKind.Utc);
                }
                return default(DateTime);
            }
        }

        public string Asset
        {
            get
            {
                // extract from PartitionKey
                if (!string.IsNullOrEmpty(this.PartitionKey))
                {
                    string[] splits = this.PartitionKey.Split(new string[] { "_" }, StringSplitOptions.RemoveEmptyEntries);
                    if (splits.Length > 0)
                    {
                        return splits[0];
                    }
                }
                return string.Empty;
            }

        }
        public bool IsBuy
        {
            get
            {
                // extract from Partition key
                if (!string.IsNullOrEmpty(this.PartitionKey))
                {
                    string[] splits = this.PartitionKey.Split(new string[] { "_" }, StringSplitOptions.RemoveEmptyEntries);
                    if (splits.Length > 1)
                    {
                        return string.Compare(splits[1], "BUY", true) == 0;
                    }
                }
                return false;
            }
        }

        #region "Back compatibility properties"

        public double High
        {
            get
            {
                if (this.Candles != null && this.Candles.Count > 0)
                {
                    return this.Candles.Select(c => c.High).Max();
                }
                return 0;
            }
        }

        public double Low
        {
            get
            {
                if (this.Candles != null && this.Candles.Count > 0)
                {
                    return this.Candles.Select(c => c.Low).Min();
                }
                return 0;
            }
        }

        public double Open
        {
            get
            {
                return (this.Candles?.FirstOrDefault()?.Open) ?? 0;
            }
        }

        public double Close
        {
            get
            {
                return (this.Candles?.LastOrDefault()?.Close) ?? 0;
            }
        }

        public int Time { get { return 0; } }

        #endregion

        public List<CandleItem> Candles { get; set; } = new List<CandleItem>();

        public void ReadEntity(IDictionary<string, EntityProperty> properties, OperationContext operationContext)
        {
            this.Candles.Clear();

            EntityProperty property;
            if(properties.TryGetValue("Data", out property))
            {
                string json = property.StringValue;
                if (!string.IsNullOrEmpty(json))
                {
                    this.Candles.AddRange(JsonConvert.DeserializeObject<List<CandleItem>>(json));
                }
            }
        }

        public IDictionary<string, EntityProperty> WriteEntity(OperationContext operationContext)
        {
            // Serialize candles
            string json = JsonConvert.SerializeObject(this.Candles);

            var dict = new Dictionary<string, EntityProperty>();
            dict.Add("Data", new EntityProperty(json));

            // For back compatibility
            dict.Add("DateTime", new EntityProperty(this.DateTime));
            dict.Add("IsBuy", new EntityProperty(this.IsBuy));
            dict.Add("Open", new EntityProperty(this.Open));
            dict.Add("Close", new EntityProperty(this.Close));
            dict.Add("High", new EntityProperty(this.High));
            dict.Add("Low", new EntityProperty(this.Low));
            dict.Add("Time", new EntityProperty(this.Time));

            return dict;
        }

        public static string GeneratePartitionKey(string assetPairId, bool isBuy, TimeInterval interval)
        {
            return $"{assetPairId}_{(isBuy ? "BUY" : "SELL")}_{interval}";
        }

        public static string GenerateRowKey(DateTime date, TimeInterval interval)
        {
            switch (interval)
            {
                case TimeInterval.Month: return $"{date.Year}";
                case TimeInterval.Day: return $"{date.Year}-{date.Month:00}";
                case TimeInterval.Hour: return $"{date.Year}-{date.Month:00}-{date.Day:00}";
                case TimeInterval.Min30:
                case TimeInterval.Min15:
                case TimeInterval.Min5:
                case TimeInterval.Minute: return $"{date.Year}-{date.Month:00}-{date.Day:00}T{date.Hour:00}";
                case TimeInterval.Sec: return $"{date.Year}-{date.Month:00}-{date.Day:00}T{date.Hour:00}:{date.Minute:00}";
                default:
                    throw new ArgumentOutOfRangeException(nameof(interval), interval, null);
            }
        }

        private static DateTime ParseDateTime(string value, DateTimeKind kind)
        {
            if (string.IsNullOrEmpty(value))
            {
                throw new ArgumentNullException(nameof(value));
            }

            string[] seg = value.Split(new char[] { '-', 'T', ':' }, StringSplitOptions.RemoveEmptyEntries);

            return new DateTime(
                seg.Length > 0 ? Int32.Parse(seg[0]) : 1900,
                seg.Length > 1 ? Int32.Parse(seg[1]) : 1,
                seg.Length > 2 ? Int32.Parse(seg[2]) : 1,
                seg.Length > 3 ? Int32.Parse(seg[3]) : 0,
                seg.Length > 4 ? Int32.Parse(seg[4]) : 0,
                seg.Length > 5 ? Int32.Parse(seg[5]) : 0,
                kind);
        }
    }
}
