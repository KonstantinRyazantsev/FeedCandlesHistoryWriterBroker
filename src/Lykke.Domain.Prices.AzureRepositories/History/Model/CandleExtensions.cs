using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using Lykke.Domain.Prices.Contracts;

namespace Lykke.Domain.Prices.AzureProvider.History.Model
{
    internal static class CandleItemExtensions
    {
        public static IFeedCandle ToCandle(this CandleItem candle, bool isBuy, DateTime baseTime, TimeInterval interval)
        {
            if (candle != null)
            {
                return new Prices.Model.FeedCandle() {
                     Open = candle.Open,
                     Close = candle.Close,
                     High = candle.High,
                     Low = candle.Low,
                     IsBuy = isBuy,
                     DateTime = baseTime.AddIntervalTicks(candle.Tick, interval)
                };
            }
            return null;
        }
    }

    internal static class CandleExtensions
    {
        public static CandleItem ToItem(this IFeedCandle candle, TimeInterval interval)
        {
            return new CandleItem()
            {
                 Open = candle.Open,
                 Close = candle.Close,
                 High = candle.High,
                 Low = candle.Low,
                 Tick = candle.DateTime.GetIntervalTick(interval)
            };
        }

        public static string PartitionKey(this IFeedCandle candle, string asset, TimeInterval interval)
        {
            if (candle == null)
            {
                throw new ArgumentNullException(nameof(candle));
            }
            return CandleTableEntity.GeneratePartitionKey(asset, candle.IsBuy, interval);
        }

        public static string RowKey(this IFeedCandle candle, TimeInterval interval)
        {
            if (candle == null)
            {
                throw new ArgumentNullException(nameof(candle));
            }
            return CandleTableEntity.GenerateRowKey(candle.DateTime, interval);
        }
    }

    internal static class CandleTableEntityExtensions
    {
        public static void MergeCandles(this CandleTableEntity entity, IEnumerable<IFeedCandle> candles, TimeInterval interval)
        {
            foreach(var candle in candles)
            {
                entity.MergeCandle(candle, interval);
            }
        }

        public static void MergeCandle(this CandleTableEntity entity, IFeedCandle candle, TimeInterval interval)
        {
            if (entity == null)
            {
                throw new ArgumentNullException(nameof(entity));
            }

            // 1. Check if candle with specified time already exist
            // 2. If found - merge, else - add to list
            //
            var tick = candle.DateTime.GetIntervalTick(interval);
            var existingCandle = entity.Candles.FirstOrDefault(ci => ci.Tick == tick);

            if (existingCandle != null)
            {
                // Merge in list
                var mergedCandle = existingCandle
                    .ToCandle(entity.IsBuy, entity.DateTime, interval)
                    .MergeWith(candle);

                entity.Candles.Remove(existingCandle);
                entity.Candles.Add(mergedCandle.ToItem(interval));
            }
            else
            {
                // Add to list
                entity.Candles.Add(candle.ToItem(interval));
            }
        }
    }
}
