using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;
using CandlesWriter.Core.Tests.Stub;
using Lykke.Domain.Prices;

namespace CandlesWriter.Core.Tests
{
    public class MidHandlerTests
    {
        [Fact]
        public void BasicGeneration()
        {
            DateTime dt = new DateTime(2017, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            const string asset = "btcusd";

            var env = new EnvironmentStub(new List<AssetPair>()
            {
                new AssetPair() { Id = asset, Accuracy = 5 }
            });
            var def = new DefaultHandler(null);
            var handler = new MidHandler(env, def);
            Queue<QuoteExt> queue = new Queue<QuoteExt>();


            handler.Handle(new QuoteExt() { AssetPair = asset, IsBuy = false, Price = 1, Timestamp = dt, PriceType = PriceType.Ask }, queue).Wait();
            Assert.Equal(1, queue.Count);
            queue.Clear();

            handler.Handle(new QuoteExt() { AssetPair = asset, IsBuy = true, Price = 2, Timestamp = dt.AddSeconds(1), PriceType = PriceType.Bid }, queue).Wait();
            Assert.Equal(2, queue.Count);

            // Check output queue
            queue.ToArray()[0].IsEqual(new QuoteExt() { AssetPair = asset, Price = 1.5, Timestamp = dt, PriceType = PriceType.Mid });

            queue.Clear();
            handler.Handle(new QuoteExt() { AssetPair = asset, IsBuy = false, Price = 3, Timestamp = dt.AddSeconds(2), PriceType = PriceType.Ask }, queue).Wait();
            Assert.Equal(2, queue.Count);

            // Check output queue
            queue.ToArray()[0].IsEqual(new QuoteExt() { AssetPair = asset, Price = 2.5, Timestamp = dt.AddSeconds(2), PriceType = PriceType.Mid });
        }

        [Fact]
        public void HandlerChecksTimestamp()
        {
            DateTime dt = new DateTime(2017, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            const string asset = "btcusd";

            var env = new EnvironmentStub(new List<AssetPair>()
            {
                new AssetPair() { Id = asset, Accuracy = 5 }
            });
            var def = new DefaultHandler(null);
            var handler = new MidHandler(env, def);
            Queue<QuoteExt> queue = new Queue<QuoteExt>();


            handler.Handle(new QuoteExt() { AssetPair = asset, IsBuy = false, Price = 1, Timestamp = dt, PriceType = PriceType.Ask }, queue).Wait();
            Assert.Equal(1, queue.Count);
            queue.Clear();

            handler.Handle(new QuoteExt() { AssetPair = asset, IsBuy = true, Price = 2, Timestamp = dt.AddSeconds(1), PriceType = PriceType.Bid }, queue).Wait();
            Assert.Equal(2, queue.Count);

            // Check output queue
            queue.ToArray()[0].IsEqual(new QuoteExt() { AssetPair = asset, Price = 1.5, Timestamp = dt, PriceType = PriceType.Mid });

            queue.Clear();
            // Old time quote should not affect computing. Though a new Mid quote is still generated.
            handler.Handle(new QuoteExt() { AssetPair = asset, IsBuy = false, Price = 3, Timestamp = dt.AddSeconds(-1), PriceType = PriceType.Ask }, queue).Wait();
            Assert.Equal(2, queue.Count);

            queue.ToArray()[0].IsEqual(new QuoteExt() { AssetPair = asset, Price = 1.5, Timestamp = dt, PriceType = PriceType.Mid });
        }

        [Fact]
        public void HandlerUsesAccuracyDictionary()
        {
            DateTime dt = new DateTime(2017, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            const string asset1 = "btcusd";
            const string asset2 = "eurusd";

            var env = new EnvironmentStub(new List<AssetPair>()
            {
                new AssetPair() { Id = asset1, Accuracy = 5 },
                new AssetPair() { Id = asset2, Accuracy = 3 }
            });
            var def = new DefaultHandler(null);
            var handler = new MidHandler(env, def);
            Queue<QuoteExt> queue = new Queue<QuoteExt>();


            handler.Handle(new QuoteExt() { AssetPair = asset1, IsBuy = false, Price = 1.12345, Timestamp = dt, PriceType = PriceType.Ask }, queue).Wait();
            handler.Handle(new QuoteExt() { AssetPair = asset1, IsBuy = true, Price = 2.54321, Timestamp = dt.AddSeconds(1), PriceType = PriceType.Bid }, queue).Wait();

            handler.Handle(new QuoteExt() { AssetPair = asset2, IsBuy = false, Price = 1.1111, Timestamp = dt, PriceType = PriceType.Ask }, queue).Wait();
            handler.Handle(new QuoteExt() { AssetPair = asset2, IsBuy = true, Price = 2.2222, Timestamp = dt.AddSeconds(1), PriceType = PriceType.Bid }, queue).Wait();

            Assert.Equal(6, queue.Count);

            // Check output queue
            var mid = queue.ToList().Where(q => q.PriceType == PriceType.Mid).ToList();
            Assert.Equal(2, mid.Count);
            mid[0].IsEqual(new QuoteExt() { AssetPair = asset1, Price = 1.83333, Timestamp = dt, PriceType = PriceType.Mid });
            mid[1].IsEqual(new QuoteExt() { AssetPair = asset2, Price = 1.667, Timestamp = dt, PriceType = PriceType.Mid });
        }
    }

    internal static class QuoteExtensions
    {
        public static bool IsEqual(this QuoteExt lhs, QuoteExt rhs)
        {
            if (lhs != null && rhs != null)
            {
                return lhs.AssetPair == rhs.AssetPair
                    && lhs.IsBuy == rhs.IsBuy
                    && lhs.Price == rhs.Price
                    && lhs.Timestamp == rhs.Timestamp
                    && lhs.PriceType == rhs.PriceType;
            }
            return false;
        }
    }
}
