using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using AzureStorage;
using Lykke.Domain.Prices;
using Lykke.Domain.Prices.Contracts;
using Lykke.Domain.Prices.Repositories;
using Lykke.Domain.Prices.AzureProvider.History.Model;

namespace Lykke.Domain.Prices.AzureProvider.History
{
    public sealed class CandleHistoryRepository : ICandleHistoryRepository
    {
        private readonly INoSQLTableStorage<CandleTableEntity> _tableStorage;

        public CandleHistoryRepository(INoSQLTableStorage<CandleTableEntity> tableStorage)
        {
            _tableStorage = tableStorage;
        }

        public async Task InsertOrMergeAsync(IEnumerable<IFeedCandle> candles, string asset, TimeInterval interval)
        {
            // Group by row
            var groups = candles
                .GroupBy(candle => new { pKey = candle.PartitionKey(asset, interval), rowKey = candle.RowKey(interval) });

            // Update rows
            foreach(var group in groups)
            {
                await InsertOrMergeAsync(group, group.Key.pKey, group.Key.rowKey, interval);
            }
        }

        private async Task InsertOrMergeAsync(IEnumerable<IFeedCandle> candles, string partitionKey, string rowKey, TimeInterval interval)
        {
            // Read row
            CandleTableEntity entity = await _tableStorage.GetDataAsync(partitionKey, rowKey);
            if (entity == null)
            {
                entity = new CandleTableEntity(partitionKey, rowKey);
            }

            // Merge all candles
            entity.MergeCandles(candles, interval);

            // Update
            await _tableStorage.InsertOrMergeAsync(entity);
        }


        public async Task InsertOrMergeAsync(IFeedCandle candle, string asset, TimeInterval interval)
        {
            // Get candle table entity
            string partitionKey = CandleTableEntity.GeneratePartitionKey(asset, candle.IsBuy, interval);
            string rowKey = CandleTableEntity.GenerateRowKey(candle.DateTime, interval);

            CandleTableEntity entity = await _tableStorage.GetDataAsync(partitionKey, rowKey);

            if (entity == null)
            {
                entity = new CandleTableEntity(partitionKey, rowKey);
            }

            // Merge candle
            entity.MergeCandle(candle, interval);

            // Update
            await _tableStorage.InsertOrMergeAsync(entity);
        }

        public async Task<IFeedCandle> GetCandleAsync(string asset, TimeInterval interval, bool isBuy, DateTime dateTime)
        {
            // 1. Get candle table entity
            string partitionKey = CandleTableEntity.GeneratePartitionKey(asset, isBuy, interval);
            string rowKey = CandleTableEntity.GenerateRowKey(dateTime, interval);

            CandleTableEntity entity = await _tableStorage.GetDataAsync(partitionKey, rowKey);

            // 2. Find required candle in candle list by tick
            if (entity != null)
            {
                var tick = dateTime.GetIntervalTick(interval);
                var candleItem = entity.Candles.FirstOrDefault(ci => ci.Tick == tick);
                return candleItem.ToCandle(isBuy, entity.DateTime, interval);
            }
            return null;
        }

        public async Task<IEnumerable<IFeedCandle>> GetCandlesAsync(string asset, TimeInterval interval, bool isBuy, DateTime from, DateTime to)
        {
            string partitionKey = CandleTableEntity.GeneratePartitionKey(asset, isBuy, interval);

            IEnumerable<CandleTableEntity> candleEntities = await _tableStorage.WhereAsync(partitionKey, from, to, ToIntervalOption.IncludeTo);

            var result = from e in candleEntities
                         select e.Candles.Select(ci => ci.ToCandle(e.IsBuy, e.DateTime, interval));

            return result.SelectMany(c => c);
        }
    }
}
