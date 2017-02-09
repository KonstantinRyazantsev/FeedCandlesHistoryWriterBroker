using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using Lykke.Domain.Prices.Contracts;

namespace Lykke.Domain.Prices.AzureProvider.History.Model
{
    internal static class CandleItemExtensions
    {
        public static ICandle ToCandle(this CandleItem candle, bool isBuy, DateTime baseTime, TimeInterval interval)
        {
            if (candle != null)
            {
                return new Prices.Model.Candle() {
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
        public static CandleItem ToItem(this ICandle candle, TimeInterval interval)
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
    }
}
