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
        public void QuotesSortedByAssetAndBuy()
        {
            var logger = new LoggerStub();
            var repo = new CandleHistoryRepositoryResolver((string asset, string tableName) =>
                {
                    return CreateStorage<CandleTableEntity>(asset, tableName, logger);
                });

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
            ProcessAllQuotes(quotes, repo, logger);

            // ... check for errors
            Assert.Equal(0, logger.Log.Where(rec => rec.Severity != LoggerStub.Severity.Info).Count());

            // 3. Read candles with repository and check count of generated candles
            //
            #region "Validation"

            CheckCountGenerated(repo, dt.AddDays(-1), dt.AddDays(1), new[] {
                new { Asset = asset1, Interval = TimeInterval.Sec, PriceType = PriceType.Bid, CountExpected = 2 },
                new { Asset = asset1, Interval = TimeInterval.Minute, PriceType = PriceType.Bid, CountExpected = 1 },
                new { Asset = asset1, Interval = TimeInterval.Min30, PriceType = PriceType.Bid, CountExpected = 1 },
                new { Asset = asset1, Interval = TimeInterval.Hour, PriceType = PriceType.Bid, CountExpected = 1 },
                new { Asset = asset1, Interval = TimeInterval.Day, PriceType = PriceType.Bid, CountExpected = 1 },
                new { Asset = asset1, Interval = TimeInterval.Month, PriceType = PriceType.Bid, CountExpected = 1 }
            });

            CheckCountGenerated(repo, new DateTime(2016, 12, 26), dt.AddDays(1), new[] {
                new { Asset = asset1, Interval = TimeInterval.Week, PriceType = PriceType.Bid, CountExpected = 1 }
            });

            CheckCountGenerated(repo, dt.AddDays(-1), dt.AddDays(1), new[] {
                new { Asset = asset1, Interval = TimeInterval.Sec, PriceType = PriceType.Ask, CountExpected = 1 },
                new { Asset = asset1, Interval = TimeInterval.Minute, PriceType = PriceType.Ask, CountExpected = 1 },
                new { Asset = asset1, Interval = TimeInterval.Min30, PriceType = PriceType.Ask, CountExpected = 1 },
                new { Asset = asset1, Interval = TimeInterval.Hour, PriceType = PriceType.Ask, CountExpected = 1 },
                new { Asset = asset1, Interval = TimeInterval.Day, PriceType = PriceType.Ask, CountExpected = 1 },
                new { Asset = asset1, Interval = TimeInterval.Month, PriceType = PriceType.Ask, CountExpected = 1 }
            });

            CheckCountGenerated(repo, new DateTime(2016, 12, 26), dt.AddDays(1), new[] {
                new { Asset = asset1, Interval = TimeInterval.Week, PriceType = PriceType.Ask, CountExpected = 1 }
            });

            CheckCountGenerated(repo, dt.AddDays(-1), dt.AddDays(1), new[] {
                new { Asset = asset1, Interval = TimeInterval.Sec, PriceType = PriceType.Mid, CountExpected = 1 },
                new { Asset = asset1, Interval = TimeInterval.Minute, PriceType = PriceType.Mid, CountExpected = 1 },
                new { Asset = asset1, Interval = TimeInterval.Min30, PriceType = PriceType.Mid, CountExpected = 1 },
                new { Asset = asset1, Interval = TimeInterval.Hour, PriceType = PriceType.Mid, CountExpected = 1 },
                new { Asset = asset1, Interval = TimeInterval.Day, PriceType = PriceType.Mid, CountExpected = 1 },
                new { Asset = asset1, Interval = TimeInterval.Month, PriceType = PriceType.Mid, CountExpected = 1 }
            });

            CheckCountGenerated(repo, new DateTime(2016, 12, 26), dt.AddDays(1), new[] {
                new { Asset = asset1, Interval = TimeInterval.Week, PriceType = PriceType.Mid, CountExpected = 1 }
            });

            #endregion
        }

        [Fact]
        public void CheckMinutesGeneration()
        {
            var logger = new LoggerStub();
            var repo = new CandleHistoryRepositoryResolver((string asset, string tableName) =>
            {
                return CreateStorage<CandleTableEntity>(asset, tableName, logger);
            });

            DateTime dt = new DateTime(2017, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            const string asset1 = "btcusd";

            // 1. Prepare incoming quotes
            //
            IEnumerable<Quote> quotes = new Quote[]
            {
                // Asset 1
                new Quote() { AssetPair = asset1, IsBuy = true, Price = 1, Timestamp = dt },                              // Second 1 // Day 1
                new Quote() { AssetPair = asset1, IsBuy = true, Price = 2, Timestamp = dt.AddMinutes(1) },
                new Quote() { AssetPair = asset1, IsBuy = true, Price = 3, Timestamp = dt.AddMinutes(2) },                // Second 2
                new Quote() { AssetPair = asset1, IsBuy = true, Price = 4, Timestamp = dt.AddMinutes(3) },                // Second 2
                new Quote() { AssetPair = asset1, IsBuy = true, Price = 5, Timestamp = dt.AddMinutes(4) },              // Second 3
            };

            // 2. Process incoming quotes
            //
            ProcessAllQuotes(quotes, repo, logger);

            // ... check for errors
            Assert.Equal(0, logger.Log.Where(rec => rec.Severity != LoggerStub.Severity.Info).Count());

            // 3. Read candles with repository and check count of generated candles
            //
            CheckCountGenerated(repo, dt.AddDays(-1), dt.AddDays(1), new[] {
                new { Asset = asset1, Interval = TimeInterval.Sec, PriceType = PriceType.Bid, CountExpected = 5 },
                new { Asset = asset1, Interval = TimeInterval.Minute, PriceType = PriceType.Bid, CountExpected = 5 }
            });
        }

        [Fact]
        public void CandlesAreInsertedAndMerged()
        {
            var logger = new LoggerStub();
            var repo = new CandleHistoryRepositoryResolver((string ast, string tableName) =>
            {
                return CreateStorage<CandleTableEntity>(ast, tableName, logger);
            });

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
                new { Asset = asset, Interval = TimeInterval.Sec, PriceType = PriceType.Bid, CountExpected = 1 }
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
            var candles = repo.GetCandlesAsync(asset, TimeInterval.Sec, PriceType.Bid, dt.AddDays(-1), dt.AddDays(1)).Result.ToArray();
            Assert.Equal(2, candles.Length);
            // ! Low value is from the first quote
            Assert.True(candles[0].IsEqual(new FeedCandle() { Open = 102, Close = 102, High = 102, Low = 101, IsBuy = true, DateTime = dt }));
            Assert.True(candles[1].IsEqual(new FeedCandle() { Open = 103, Close = 103, High = 103, Low = 103, IsBuy = true, DateTime = dt.AddSeconds(1) }));
        }

        private void CheckCountGenerated(ICandleHistoryRepository repo, DateTime from, DateTime to, dynamic[] requirements)
        {
            foreach (var req in requirements)
            {
                IEnumerable<IFeedCandle> candles = repo.GetCandlesAsync(req.Asset, req.Interval, req.PriceType, from, to).Result;
                Assert.Equal(req.CountExpected, candles.Count());
            }
        }

        private INoSQLTableStorage<T> CreateStorage<T>(string asset, string tableName, ILog logger, bool clear = true) where T : class, ITableEntity, new()
        {
            if (asset != "btcusd")
            {
                throw new AppSettingException("");
            }

            var table = new AzureTableStorage<T>("UseDevelopmentStorage=true;", tableName, logger);
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
            do {
                entities.Clear();
                table.GetDataByChunksAsync(collection => entities.AddRange(collection)).Wait();
                entities.ForEach(e => table.DeleteAsync(e).Wait());
            } while (entities.Count > 0);
        }

        private static void ProcessAllQuotes(IEnumerable<Quote> quotes, ICandleHistoryRepository repo, LoggerStub logger)
        {
            var env = new EnvironmentStub(new List<AssetPair>() {
                new AssetPair() { Id = "btcusd", Accuracy = 3 },
                //new AssetPair() { Id = "btcrub", Accuracy = 3 }
            });
            var controller = new CandleGenerationController(repo, logger, "test component", env);

            var tasks = new List<Task>();
            foreach (var quote in quotes)
            {
                var task = controller.HandleQuote(quote);
                tasks.Add(task);
            }
            Task.WaitAll(tasks.ToArray());

            // ... signal controller to process quotes
            controller.Tick();

            int counter = 0;
            while (counter < 5)
            {
                Task.Delay(1000).Wait(); // Wait while produce task is finished.
                if (controller.QueueLength == 0)
                {
                    break;
                }
                counter++;
            }
            Assert.Equal(0, controller.QueueLength);
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

    internal class EnvironmentStub : IEnvironment
    {
        private IEnumerable<AssetPair> assets;

        public EnvironmentStub(IEnumerable<AssetPair> assets)
        {
            this.assets = assets;
        }

        public Task<IAssetPair> GetAssetPair(string asset)
        {
            return Task.FromResult(this.assets.Where(a => a.Id == asset).FirstOrDefault() as IAssetPair);
        }
    }
}
