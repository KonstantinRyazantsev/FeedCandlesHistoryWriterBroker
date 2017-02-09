using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Lykke.Domain.Prices;
using Lykke.Domain.Prices.Contracts;

namespace CandlesWriter.Core.Tests.Stub
{
    public class CandleHistoryRepositoryStub : ICandleHistoryRepository
    {
        private List<StoreItem> storage = new List<StoreItem>();

        public IReadOnlyList<StoreItem> Stored { get { return this.storage; } }

        public Task<ICandle> GetCandleAsync(string asset, TimeInterval interval, bool isBuy, DateTime dateTime)
        {
            throw new NotImplementedException();
        }

        public Task<IEnumerable<ICandle>> GetCandlesAsync(string asset, TimeInterval interval, bool isBuy, DateTime from, DateTime to)
        {
            throw new NotImplementedException();
        }

        public Task InsertOrMergeAsync(ICandle candle, string asset, TimeInterval interval)
        {
            this.storage.Add(new StoreItem()
            {
                candle = candle,
                asset = asset,
                interval = interval
            });
            return Task.FromResult(0);
        }

        public struct StoreItem
        {
            public ICandle candle;
            public string asset;
            public TimeInterval interval;
        }
    }
}
