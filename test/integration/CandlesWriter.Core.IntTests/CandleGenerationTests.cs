using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Table;
using Xunit;
using AzureRepositories.Candles;
using AzureStorage;
using AzureStorage.Tables;
using Lykke.Domain.Prices;
using Lykke.Domain.Prices.Model;
using Lykke.Domain.Prices.Contracts;
using Lykke.Domain.Prices.Repositories;
using CandlesWriter.Core.IntTests.Stub;
using Common.Log;

namespace CandlesWriter.Core.IntTests
{
    public class CandleGenerationTests
    {
        [Fact]
        public void RepositorySupportsLegacyRows()
        {
            // Create record with legacy row
            //
            var logger = new LoggerStub();
            DateTime dt = new DateTime(2016, 11, 30, 0, 0, 0, DateTimeKind.Utc);
            var storage = CreateStorage<Legacy.FeedCandleEntity>(logger);

            var entity = new Legacy.FeedCandleEntity()
            {
                PartitionKey = "BTCCHF_BUY_Hour",
                RowKey = "2016-11-30",
                Open = 740.508,
                Close = 755.11,
                High = 755.491,
                Low = 738.679,
                IsBuy = true,
                Time = 0,
                DateTime = dt,
                Data = "[{\"O\":740.508,\"C\":741.843,\"H\":742.596,\"L\":738.679,\"T\":0},{\"O\":741.865,\"C\":741.785,\"H\":742.731,\"L\":740.709,\"T\":1},{\"O\":753.497,\"C\":755.11,\"H\":755.491,\"L\":753.486,\"T\":23}]"
            };

            storage.InsertOrReplaceAsync(entity).Wait();

            #region "Read created row with new repository's GetCandles method"

            var repo = new CandleHistoryRepository(CreateStorage<CandleTableEntity>(logger, clear: false));
            var candles = repo.GetCandlesAsync("BTCCHF", TimeInterval.Hour, true, dt.AddDays(-1), dt.AddDays(1)).Result.ToArray();

            // Does not read data, and does not throw exceptions.
            Assert.Equal(0, candles.Length);

            #endregion

            #region "Read created row with new repository's GetCandle method"

            IFeedCandle candle1 = repo.GetCandleAsync("BTCCHF", TimeInterval.Hour, true, dt).Result;
            // Does not read data, and does not throw exceptions.
            Assert.Null(candle1);

            IFeedCandle candle2 = repo.GetCandleAsync("BTCCHF", TimeInterval.Hour, true, dt.AddHours(1)).Result;
            Assert.Null(candle2);

            #endregion
        }

        [Fact]
        public void QuotesSortedByAssetAndBuy()
        {
            var logger = new LoggerStub();
            var storage = CreateStorage<CandleTableEntity>(logger);
            var repo = new CandleHistoryRepository(storage);

            DateTime dt = new DateTime(2017, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            const string asset1 = "btcusd";
            const string asset2 = "btcrub";

            // 1. Prepare incoming quotes
            //
            IEnumerable<Quote> quotes = new Quote[]
            {
                // Asset 1
                new Quote() { AssetPair = asset1, IsBuy = true, Price = 101, Timestamp = dt },                              // Second 1 // Day 1
                new Quote() { AssetPair = asset1, IsBuy = true, Price = 101, Timestamp = dt.AddMilliseconds(1) },
                new Quote() { AssetPair = asset1, IsBuy = true, Price = 100, Timestamp = dt.AddSeconds(2) },                // Second 2
                new Quote() { AssetPair = asset1, IsBuy = false, Price = 100, Timestamp = dt.AddSeconds(10) },              // Second 3
                // Asset 2
                new Quote() { AssetPair = asset2, IsBuy = true, Price = 101, Timestamp = dt.AddDays(1) },                   // Second 1 // Day 2
                new Quote() { AssetPair = asset2, IsBuy = true, Price = 101, Timestamp = dt.AddDays(1).AddMilliseconds(1) },
                new Quote() { AssetPair = asset2, IsBuy = true, Price = 100, Timestamp = dt.AddDays(1).AddSeconds(2) },     // Second 2
                new Quote() { AssetPair = asset2, IsBuy = false, Price = 100, Timestamp = dt.AddDays(1).AddSeconds(10) },   // Second 3
            };

            // 2. Process incoming quotes
            //
            ProcessAllQuotes( quotes, repo, logger);

            // ... check for no errors
            Assert.Equal(0, logger.Log.Where(rec => rec.Severity != LoggerStub.Severity.Info).Count());

            // 3. Read candles with repository and check count of generated candles
            //
            #region "Validation"

            CheckCountGenerated(repo, dt.AddDays(-1), dt.AddDays(1), new[] {
                new { Asset = asset1, Interval = TimeInterval.Sec, IsBuy = true, CountExpected = 2 },
                new { Asset = asset1, Interval = TimeInterval.Minute, IsBuy = true, CountExpected = 1 },
                new { Asset = asset1, Interval = TimeInterval.Min5, IsBuy = true, CountExpected = 1 },
                new { Asset = asset1, Interval = TimeInterval.Min15, IsBuy = true, CountExpected = 1 },
                new { Asset = asset1, Interval = TimeInterval.Min30, IsBuy = true, CountExpected = 1 },
                new { Asset = asset1, Interval = TimeInterval.Hour, IsBuy = true, CountExpected = 1 },
                new { Asset = asset1, Interval = TimeInterval.Day, IsBuy = true, CountExpected = 1 },
                new { Asset = asset1, Interval = TimeInterval.Month, IsBuy = true, CountExpected = 1 }
            });

            CheckCountGenerated(repo, dt.AddDays(-1), dt.AddDays(1), new[] {
                new { Asset = asset1, Interval = TimeInterval.Sec, IsBuy = false, CountExpected = 1 },
                new { Asset = asset1, Interval = TimeInterval.Minute, IsBuy = false, CountExpected = 1 },
                new { Asset = asset1, Interval = TimeInterval.Min5, IsBuy = false, CountExpected = 1 },
                new { Asset = asset1, Interval = TimeInterval.Min15, IsBuy = false, CountExpected = 1 },
                new { Asset = asset1, Interval = TimeInterval.Min30, IsBuy = false, CountExpected = 1 },
                new { Asset = asset1, Interval = TimeInterval.Hour, IsBuy = false, CountExpected = 1 },
                new { Asset = asset1, Interval = TimeInterval.Day, IsBuy = false, CountExpected = 1 },
                new { Asset = asset1, Interval = TimeInterval.Month, IsBuy = false, CountExpected = 1 }
            });

            CheckCountGenerated(repo, dt.AddDays(-1), dt.AddDays(2), new[] {
                new { Asset = asset2, Interval = TimeInterval.Sec, IsBuy = true, CountExpected = 2 },
                new { Asset = asset2, Interval = TimeInterval.Minute, IsBuy = true, CountExpected = 1 },
                new { Asset = asset2, Interval = TimeInterval.Min5, IsBuy = true, CountExpected = 1 },
                new { Asset = asset2, Interval = TimeInterval.Min15, IsBuy = true, CountExpected = 1 },
                new { Asset = asset2, Interval = TimeInterval.Min30, IsBuy = true, CountExpected = 1 },
                new { Asset = asset2, Interval = TimeInterval.Hour, IsBuy = true, CountExpected = 1 },
                new { Asset = asset2, Interval = TimeInterval.Day, IsBuy = true, CountExpected = 1 },
                new { Asset = asset2, Interval = TimeInterval.Month, IsBuy = true, CountExpected = 1 }
            });

            CheckCountGenerated(repo, dt.AddDays(-1), dt.AddDays(2), new[] {
                new { Asset = asset2, Interval = TimeInterval.Sec, IsBuy = false, CountExpected = 1 },
                new { Asset = asset2, Interval = TimeInterval.Minute, IsBuy = false, CountExpected = 1 },
                new { Asset = asset2, Interval = TimeInterval.Min5, IsBuy = false, CountExpected = 1 },
                new { Asset = asset2, Interval = TimeInterval.Min15, IsBuy = false, CountExpected = 1 },
                new { Asset = asset2, Interval = TimeInterval.Min30, IsBuy = false, CountExpected = 1 },
                new { Asset = asset2, Interval = TimeInterval.Hour, IsBuy = false, CountExpected = 1 },
                new { Asset = asset2, Interval = TimeInterval.Day, IsBuy = false, CountExpected = 1 },
                new { Asset = asset2, Interval = TimeInterval.Month, IsBuy = false, CountExpected = 1 }
            });

            #endregion
        }

        [Fact]
        public void CandlesAreInsertedAndMerged()
        {
            var logger = new LoggerStub();
            var storage = CreateStorage<CandleTableEntity>(logger);
            var repo = new CandleHistoryRepository(storage);

            DateTime dt = new DateTime(2017, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            const string asset = "btcusd";

            // 1. Prepare incoming quotes
            //
            IEnumerable<Quote> quotes = new Quote[]
            {
                new Quote() { AssetPair = asset, IsBuy = true, Price = 101, Timestamp = dt }, // Second 1
            };

            // 2. Process incoming quotes
            //
            ProcessAllQuotes(quotes, repo, logger);

            // ... check for no errors
            Assert.Equal(0, logger.Log.Where(rec => rec.Severity != LoggerStub.Severity.Info).Count());

            // 3. Read candles with repository and check count of generated candles
            //
            CheckCountGenerated(repo, dt.AddDays(-1), dt.AddDays(1), new[] {
                new { Asset = asset, Interval = TimeInterval.Sec, IsBuy = true, CountExpected = 1 },
            });

            // 4. Send more quotes that should generate two candles. One candle should be merged and one candle should be added.
            //
            IEnumerable<Quote> quotes2 = new Quote[]
            {
                new Quote() { AssetPair = asset, IsBuy = true, Price = 102, Timestamp = dt }, // Second 1 (updated)
                new Quote() { AssetPair = asset, IsBuy = true, Price = 103, Timestamp = dt.AddSeconds(1) }, // Second 2
            };
            ProcessAllQuotes(quotes2, repo, logger);

            // ... check for no errors
            Assert.Equal(0, logger.Log.Where(rec => rec.Severity != LoggerStub.Severity.Info).Count());
            
            // 5. Validate merging
            //
            var candles = repo.GetCandlesAsync(asset, TimeInterval.Sec, true, dt.AddDays(-1), dt.AddDays(1)).Result.ToArray();
            Assert.Equal(2, candles.Length);
            // ! Low value is from the first quote
            Assert.True(candles[0].IsEqual(new FeedCandle() { Open = 102, Close = 102, High = 102, Low = 101, IsBuy = true, DateTime = dt }));
            Assert.True(candles[1].IsEqual(new FeedCandle() { Open = 103, Close = 103, High = 103, Low = 103, IsBuy = true, DateTime = dt.AddSeconds(1) }));
        }

        private void CheckCountGenerated(ICandleHistoryRepository repo, DateTime from, DateTime to, dynamic[] requirements)
        {
            foreach(var req in requirements)
            {
                IEnumerable<IFeedCandle> candles = repo.GetCandlesAsync(req.Asset, req.Interval, req.IsBuy, from, to).Result;
                Assert.Equal(req.CountExpected, candles.Count());
            }
        }

        private INoSQLTableStorage<T> CreateStorage<T>(ILog logger, bool clear = true) where T : class, ITableEntity, new()
        {
            var table = new AzureTableStorage<T>("UseDevelopmentStorage=true;", "CandlesHistoryTest", logger);
            if (clear)
            {
                ClearTable(table);
            }
            return table;

            // NoSqlTableInMemory does not implement ScanDataAsync method
            //return new NoSqlTableInMemory<T>();
        }

        private static void ClearTable<T>(AzureTableStorage<T> table) where T : class, ITableEntity, new()
        {
            var entities = new List<T>();
            do
            {
                entities.Clear();
                table.GetDataByChunksAsync(collection => entities.AddRange(collection)).Wait();
                entities.ForEach(e => table.DeleteAsync(e).Wait());
            } while (entities.Count > 0);
        }

        private static void ProcessAllQuotes(IEnumerable<Quote> quotes, ICandleHistoryRepository repo, LoggerStub logger)
        {
            var controller = new CandleGenerationController(repo, logger, "test component");

            var tasks = new List<Task>();
            foreach (var quote in quotes)
            {
                var task = controller.ConsumeQuote(quote);
                tasks.Add(task);
            }
            Task.WaitAll(tasks.ToArray());

            // ... signal controller to process quotes
            controller.Tick().Wait();
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
