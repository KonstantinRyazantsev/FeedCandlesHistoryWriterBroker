using System;
using System.Collections.Generic;
using System.Linq;
using Xunit;
using Lykke.Domain.Prices;
using Lykke.Domain.Prices.Model;
using Lykke.Domain.Prices.Contracts;

namespace CandlesWriter.Core.Tests
{
    public class CandleGeneratorTests
    {
        [Fact]
        public void EmptyReturnedIfNullPassed()
        {
            IEnumerable<QuoteExt> quotes = null;
            var generator = new CandleGenerator();
            var candles = generator.Generate(quotes, TimeInterval.Sec, PriceType.Ask);
            Assert.NotNull(candles);
            Assert.Equal(0, candles.Count());
        }

        [Fact]
        public void EmptyReturnedIfEmptyCollectionPassed()
        {
            IEnumerable<QuoteExt> quotes = new QuoteExt[0];
            var generator = new CandleGenerator();
            var candles = generator.Generate(quotes, TimeInterval.Sec, PriceType.Ask);
            Assert.NotNull(candles);
            Assert.Equal(0, candles.Count());
        }

        [Fact]
        public void CandlesAreGrouped()
        {
            DateTime dt = new DateTime(2017, 1, 1);
            var generator = new CandleGenerator();

            var quotes = new QuoteExt[]
            {
                new QuoteExt { AssetPair = "BTCUSD", IsBuy = true, Price = 1.1, Timestamp = dt, PriceType = PriceType.Bid },
                new QuoteExt { AssetPair = "BTCUSD", IsBuy = true, Price = 0.8, Timestamp = dt, PriceType = PriceType.Bid }
            };

            var candles = generator.Generate(quotes, TimeInterval.Sec, PriceType.Bid);
            Assert.Equal(1, candles.Count());

            quotes = new QuoteExt[]
            {
                new QuoteExt { AssetPair = "BTCUSD", IsBuy = true, Price = 1.1, Timestamp = dt.AddMinutes(2).AddSeconds(1), PriceType = PriceType.Bid },
                new QuoteExt { AssetPair = "BTCUSD", IsBuy = true, Price = 0.8, Timestamp = dt.AddMinutes(2).AddSeconds(8), PriceType = PriceType.Bid }
            };

            candles = generator.Generate(quotes, TimeInterval.Minute, PriceType.Bid);
            Assert.Equal(1, candles.Count());

            quotes = new QuoteExt[]
            {
                new QuoteExt { AssetPair = "BTCUSD", IsBuy = true, Price = 1.1, Timestamp = dt.AddHours(1).AddMinutes(1), PriceType = PriceType.Bid },
                new QuoteExt { AssetPair = "BTCUSD", IsBuy = true, Price = 0.8, Timestamp = dt.AddHours(2).AddMinutes(8), PriceType = PriceType.Bid }
            };

            candles = generator.Generate(quotes, TimeInterval.Hour, PriceType.Bid);
            Assert.Equal(2, candles.Count());

            quotes = new QuoteExt[]
            {
                new QuoteExt { AssetPair = "BTCUSD", IsBuy = true, Price = 1.1, Timestamp = dt.AddDays(1).AddHours(1), PriceType = PriceType.Bid },
                new QuoteExt { AssetPair = "BTCUSD", IsBuy = true, Price = 0.8, Timestamp = dt.AddDays(2).AddHours(8), PriceType = PriceType.Bid }
            };

            candles = generator.Generate(quotes, TimeInterval.Day, PriceType.Bid);
            Assert.Equal(2, candles.Count());

            quotes = new QuoteExt[]
            {
                new QuoteExt { AssetPair = "BTCUSD", IsBuy = true, Price = 1.1, Timestamp = dt.AddMonths(-2).AddDays(1), PriceType = PriceType.Bid },
                new QuoteExt { AssetPair = "BTCUSD", IsBuy = true, Price = 0.8, Timestamp = dt.AddMonths(-1).AddDays(8), PriceType = PriceType.Bid }
            };

            candles = generator.Generate(quotes, TimeInterval.Month, PriceType.Bid);
            Assert.Equal(2, candles.Count());

            quotes = new QuoteExt[]
            {
                new QuoteExt { AssetPair = "BTCUSD", IsBuy = true, Price = 1.1, Timestamp = dt.AddMonths(1).AddDays(1), PriceType = PriceType.Bid },
                new QuoteExt { AssetPair = "BTCUSD", IsBuy = true, Price = 0.8, Timestamp = dt.AddMonths(2).AddDays(8), PriceType = PriceType.Bid }
            };

            candles = generator.Generate(quotes, TimeInterval.Month, PriceType.Bid);
            Assert.Equal(2, candles.Count());
        }

        [Fact]
        public void SecondCandlesAreGrouped()
        {
            DateTime dt = new DateTime(2017, 1, 1);

            var quotes = new QuoteExt[]
            {
                new QuoteExt { AssetPair = "BTCUSD", IsBuy = true, Price = 1, Timestamp = dt.AddMilliseconds(50), PriceType = PriceType.Bid },
                new QuoteExt { AssetPair = "BTCUSD", IsBuy = true, Price = 2, Timestamp = dt.AddMilliseconds(100), PriceType = PriceType.Bid },
                new QuoteExt { AssetPair = "BTCUSD", IsBuy = true, Price = 3, Timestamp = dt.AddSeconds(5), PriceType = PriceType.Bid },
                new QuoteExt { AssetPair = "BTCUSD", IsBuy = true, Price = 4, Timestamp = dt.AddSeconds(15), PriceType = PriceType.Bid },
                new QuoteExt { AssetPair = "BTCUSD", IsBuy = true, Price = 5, Timestamp = dt.AddSeconds(15).AddMilliseconds(10), PriceType = PriceType.Bid },
                new QuoteExt { AssetPair = "BTCUSD", IsBuy = true, Price = 6, Timestamp = dt.AddSeconds(15).AddMilliseconds(11), PriceType = PriceType.Bid }
            };

            var candles = new CandleGenerator().Generate(quotes, TimeInterval.Sec, PriceType.Bid).ToArray();

            Assert.Equal(3, candles.Length);
            Assert.True(candles[0].IsEqual(new FeedCandle() { Open = 1, Close = 2, High = 2, Low = 1, IsBuy = true, DateTime = dt }));
            Assert.True(candles[1].IsEqual(new FeedCandle() { Open = 3, Close = 3, High = 3, Low = 3, IsBuy = true, DateTime = dt.AddSeconds(5) }));
            Assert.True(candles[2].IsEqual(new FeedCandle() { Open = 4, Close = 6, High = 6, Low = 4, IsBuy = true, DateTime = dt.AddSeconds(15) }));
        }

        [Fact]
        public void MinutesCandlesAreGrouped()
        {
            DateTime dt = new DateTime(2017, 1, 1);

            var quotes = new QuoteExt[]
            {
                new QuoteExt { AssetPair = "BTCUSD", IsBuy = true, Price = 1, Timestamp = dt, PriceType = PriceType.Bid },
                new QuoteExt { AssetPair = "BTCUSD", IsBuy = true, Price = 2, Timestamp = dt.AddMinutes(1), PriceType = PriceType.Bid },
                new QuoteExt { AssetPair = "BTCUSD", IsBuy = true, Price = 3, Timestamp = dt.AddMinutes(2), PriceType = PriceType.Bid },
                new QuoteExt { AssetPair = "BTCUSD", IsBuy = true, Price = 4, Timestamp = dt.AddMinutes(3), PriceType = PriceType.Bid },
                new QuoteExt { AssetPair = "BTCUSD", IsBuy = true, Price = 5, Timestamp = dt.AddMinutes(4), PriceType = PriceType.Bid },
                new QuoteExt { AssetPair = "BTCUSD", IsBuy = true, Price = 6, Timestamp = dt.AddMinutes(5), PriceType = PriceType.Bid }
            };

            var candles = new CandleGenerator().Generate(quotes, TimeInterval.Sec, PriceType.Bid).ToArray();

            Assert.Equal(6, candles.Length);
            Assert.True(candles[0].IsEqual(new FeedCandle() { Open = 1, Close = 1, High = 1, Low = 1, IsBuy = true, DateTime = dt }));
            Assert.True(candles[1].IsEqual(new FeedCandle() { Open = 2, Close = 2, High = 2, Low = 2, IsBuy = true, DateTime = dt.AddMinutes(1) }));
            Assert.True(candles[2].IsEqual(new FeedCandle() { Open = 3, Close = 3, High = 3, Low =3, IsBuy = true, DateTime = dt.AddMinutes(2) }));
        }
    }

    internal static class CandleExtensions
    {
        public static bool IsEqual(this IFeedCandle candle, IFeedCandle other)
        {
            if (other != null && candle != null)
            {
                return candle.DateTime == other.DateTime
                    && candle.Open == other.Open
                    && candle.Close == other.Close
                    && candle.High == other.High
                    && candle.Low == other.Low
                    && candle.IsBuy == other.IsBuy;
            }
            return false;
        }
    }
}
