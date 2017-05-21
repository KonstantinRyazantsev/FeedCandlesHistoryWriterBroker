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
using System.Diagnostics;

namespace CandlesWriter.Core.IntTests
{
    public class CandleGenerationTests_Threading
    {
        [Fact]
        public void ControllerHandlesRepositoryExceptions()
        {
            const string asset1 = "btcusd";
            DateTime dt = new DateTime(2017, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            var logger = new LoggerStub();

            ClearTable(asset1, new string[] { "sec", "minute", "min30", "hour", "day", "week", "month" });
            var repo = new CandleHistoryRepositoryResolver((string asset, string tableName) =>
            {
                return CreateStorage<CandleTableEntity>(asset, tableName, logger, 2, clear: false);
            });

            // 2. Process incoming quotes
            //
            var env = new EnvironmentStub(new List<AssetPair>() {
                new AssetPair() { Id = asset1, Accuracy = 3 }
            });
            var controller = new CandleGenerationController(repo, logger, "test component", env);

            for (int i = 0; i < 5; i++)
            {
                var tasks = new List<Task>();

                // pass a quote and signal controller to process quotes
                var q = new Quote() { AssetPair = asset1, IsBuy = true, Price = i + 1, Timestamp = dt.AddMinutes(i) };
                var task = controller.HandleQuote(q);
                tasks.Add(task);

                Task.WaitAll(tasks.ToArray());

                // ... signal controller to process quotes
                controller.Tick();

                int counter = 0;
                while (counter < 60)
                {
                    Task.Delay(1000).Wait(); // Wait while produce task is finished.
                    if (controller.QueueLength == 0)
                    {
                        break;
                    }
                    counter++;
                }
            }
 
            Assert.Equal(0, controller.QueueLength);

            controller.Stop();
            // ... check for errors
            //Assert.Equal(0, logger.Log.Where(rec => rec.Severity != LoggerStub.Severity.Info).Count());

            var logs = logger.Log.Where(rec => rec.Severity != LoggerStub.Severity.Info);

            // 3. Read candles with repository and check count of generated candles
            //
            IEnumerable<IFeedCandle> candles = repo.GetCandlesAsync(asset1, TimeInterval.Minute, PriceType.Bid, dt.AddDays(-1), dt.AddDays(1)).Result;
            Assert.Equal(5, candles.Count());

            candles = repo.GetCandlesAsync(asset1, TimeInterval.Sec, PriceType.Bid, dt.AddDays(-1), dt.AddDays(1)).Result;
            Assert.Equal(5, candles.Count());
        }

        private INoSQLTableStorage<T> CreateStorage<T>(string asset, string tableName, ILog logger, int exFreq = 0, bool clear = true) where T : class, ITableEntity, new()
        {
            if (asset != "btcusd")
            {
                throw new AppSettingException("");
            }

            //var table = new AzureTableStorage<T>("UseDevelopmentStorage=true;", tableName, logger);
            //if (clear)
            //{
            //    ClearTable(table);
            //}

            //return new AzureTableStorageWrapper<T>("UseDevelopmentStorage=true;", tableName, logger, exFreq);

            // NoSqlTableInMemory does not implement ScanDataAsync method
            return new NoSqlTableInMemory<T>();
        }

        private static void ClearTable(string asset, string[] tables)
        {
            //foreach (var tableName in tables)
            //{
            //    var table = new AzureTableStorage<CandleTableEntity>("UseDevelopmentStorage=true;", tableName, null);
            //    ClearTable(table);
            //}
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
    }
}
