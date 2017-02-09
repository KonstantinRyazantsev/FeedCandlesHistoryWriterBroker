using System;
using System.Collections.Generic;
using System.Linq;
using Xunit;
using Lykke.Domain.Prices;
using Lykke.Domain.Prices.Model;

namespace CandlesWriter.Core.Tests
{
    public class CandleGeneratorTests
    {
        [Fact]
        public void EmptyReturnedIfNullPassed()
        {
            IEnumerable<Quote> quotes = null;
            var generator = new CandleGenerator();
            var candles = generator.Generate(quotes, TimeInterval.Sec);
            Assert.NotNull(candles);
            Assert.Equal(0, candles.Count());
        }

        [Fact]
        public void EmptyReturnedIfEmptyCollectionPassed()
        {
            IEnumerable<Quote> quotes = new Quote[0];
            var generator = new CandleGenerator();
            var candles = generator.Generate(quotes, TimeInterval.Sec);
            Assert.NotNull(candles);
            Assert.Equal(0, candles.Count());
        }

        [Fact]
        public void CandlesAreGrouped()
        {
            DateTime dt = new DateTime(2017, 1, 1);
            var generator = new CandleGenerator();

            var quotes = new Quote[]
            {
                new Quote { AssetPair = "BTCUSD", IsBuy = true, Price = 1.1, Timestamp = dt},
                new Quote { AssetPair = "BTCUSD", IsBuy = true, Price = 0.8, Timestamp = dt}
            };

            var candles = generator.Generate(quotes, TimeInterval.Sec);
            Assert.Equal(1, candles.Count());

            quotes = new Quote[]
            {
                new Quote { AssetPair = "BTCUSD", IsBuy = true, Price = 1.1, Timestamp = dt.AddMinutes(2).AddSeconds(1)},
                new Quote { AssetPair = "BTCUSD", IsBuy = true, Price = 0.8, Timestamp = dt.AddMinutes(2).AddSeconds(8)}
            };

            candles = generator.Generate(quotes, TimeInterval.Minute);
            Assert.Equal(1, candles.Count());

            quotes = new Quote[]
            {
                new Quote { AssetPair = "BTCUSD", IsBuy = true, Price = 1.1, Timestamp = dt.AddHours(1).AddMinutes(1)},
                new Quote { AssetPair = "BTCUSD", IsBuy = true, Price = 0.8, Timestamp = dt.AddHours(2).AddMinutes(8)}
            };

            candles = generator.Generate(quotes, TimeInterval.Hour);
            Assert.Equal(2, candles.Count());

            quotes = new Quote[]
            {
                new Quote { AssetPair = "BTCUSD", IsBuy = true, Price = 1.1, Timestamp = dt.AddDays(1).AddHours(1)},
                new Quote { AssetPair = "BTCUSD", IsBuy = true, Price = 0.8, Timestamp = dt.AddDays(2).AddHours(8)}
            };

            candles = generator.Generate(quotes, TimeInterval.Day);
            Assert.Equal(2, candles.Count());

            quotes = new Quote[]
            {
                new Quote { AssetPair = "BTCUSD", IsBuy = true, Price = 1.1, Timestamp = dt.AddMonths(-2).AddDays(1)},
                new Quote { AssetPair = "BTCUSD", IsBuy = true, Price = 0.8, Timestamp = dt.AddMonths(-1).AddDays(8)}
            };

            candles = generator.Generate(quotes, TimeInterval.Month);
            Assert.Equal(2, candles.Count());

            quotes = new Quote[]
            {
                new Quote { AssetPair = "BTCUSD", IsBuy = true, Price = 1.1, Timestamp = dt.AddMonths(1).AddDays(1)},
                new Quote { AssetPair = "BTCUSD", IsBuy = true, Price = 0.8, Timestamp = dt.AddMonths(2).AddDays(8)}
            };

            candles = generator.Generate(quotes, TimeInterval.Month);
            Assert.Equal(2, candles.Count());
        }

        [Fact]
        public void SecondCandlesAreGrouped()
        {
            DateTime dt = new DateTime(2017, 1, 1);

            var quotes = new Quote[]
            {
                new Quote { AssetPair = "BTCUSD", IsBuy = true, Price = 1, Timestamp = dt.AddMilliseconds(50) },
                new Quote { AssetPair = "BTCUSD", IsBuy = true, Price = 2, Timestamp = dt.AddMilliseconds(100) },
                new Quote { AssetPair = "BTCUSD", IsBuy = true, Price = 3, Timestamp = dt.AddSeconds(5) },
                new Quote { AssetPair = "BTCUSD", IsBuy = true, Price = 4, Timestamp = dt.AddSeconds(15) },
                new Quote { AssetPair = "BTCUSD", IsBuy = true, Price = 5, Timestamp = dt.AddSeconds(15).AddMilliseconds(10) },
                new Quote { AssetPair = "BTCUSD", IsBuy = true, Price = 6, Timestamp = dt.AddSeconds(15).AddMilliseconds(11) }
            };

            var candles = new CandleGenerator().Generate(quotes, TimeInterval.Sec).ToArray();

            Assert.Equal(3, candles.Length);
            Assert.True(candles[0].Equals(new Candle() { Open = 1, Close = 2, High = 2, Low = 1, IsBuy = true, DateTime = dt }));
            Assert.True(candles[1].Equals(new Candle() { Open = 3, Close = 3, High = 3, Low = 3, IsBuy = true, DateTime = dt.AddSeconds(5) }));
            Assert.True(candles[2].Equals(new Candle() { Open = 4, Close = 6, High = 6, Low = 4, IsBuy = true, DateTime = dt.AddSeconds(15) }));
        }
    }
}
