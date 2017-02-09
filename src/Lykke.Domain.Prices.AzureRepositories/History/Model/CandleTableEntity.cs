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

    //public class CandleTableEntity : TableEntity //, ICandle
    //{
    //    public CandleTableEntity()
    //    {
    //    }

    //    public CandleTableEntity(string partitionKey, string rowKey)
    //        : base(partitionKey, rowKey)
    //    {
    //    }

    //    public string Data
    //    {
    //        get
    //        {
    //            // serialize candles
    //            return JsonConvert.SerializeObject(this.Candles);
    //        }
    //        set
    //        {
    //            // deserialize candles
    //            this.Candles.Clear();
    //            if (!string.IsNullOrEmpty(value))
    //            {
    //                this.Candles.AddRange(JsonConvert.DeserializeObject<List<CandleItem>>(value));
    //            }
    //        }
    //    }

    //    //public double Open { get; set; }
    //    //public double Close { get; set; }
    //    //public double High { get; set; }
    //    //public double Low { get; set; }
    //    public DateTime DateTime
    //    {
    //        get
    //        {
    //            // extract from RowKey + Interval from PKey
    //            if (!string.IsNullOrEmpty(this.PartitionKey))
    //            {

    //            }
    //            return default(DateTime);
    //        }
    //    }

    //    public string Asset
    //    {
    //        get
    //        {
    //            // extract from PartitionKey
    //            if (!string.IsNullOrEmpty(this.PartitionKey))
    //            {

    //            }
    //            return string.Empty;
    //        }

    //    }
    //    public bool IsBuy
    //    {
    //        get
    //        {
    //            // extract from Partition key
    //            if (!string.IsNullOrEmpty(this.PartitionKey))
    //            {

    //            }
    //            return false;
    //        }
    //        set
    //        {
    //            // Ignore
    //        }
    //    }

    //    public List<CandleItem> Candles { get; set; } = new List<CandleItem>();
    //    //public int Time { get; set; }

    //    public static string GeneratePartitionKey(string assetPairId, bool isBuy, TimeInterval interval)
    //    {
    //        return $"{assetPairId}_{(isBuy ? "BUY" : "SELL")}_{interval}";
    //    }

    //    public static string GenerateRowKey(DateTime date, TimeInterval interval)
    //    {
    //        switch (interval)
    //        {
    //            case TimeInterval.Month: return $"{date.Year}";
    //            case TimeInterval.Day: return $"{date.Year}-{date.Month:00}";
    //            case TimeInterval.Hour: return $"{date.Year}-{date.Month:00}-{date.Day:00}";
    //            case TimeInterval.Min30:
    //            case TimeInterval.Min15:
    //            case TimeInterval.Min5:
    //            case TimeInterval.Minute: return $"{date.Year}-{date.Month:00}-{date.Day:00}T{date.Hour:00}";
    //            case TimeInterval.Sec: return $"{date.Year}-{date.Month:00}-{date.Day:00}T{date.Hour:00}:{date.Minute:00}";
    //            default:
    //                throw new ArgumentOutOfRangeException(nameof(interval), interval, null);
    //        }
    //    }
    //}
}
