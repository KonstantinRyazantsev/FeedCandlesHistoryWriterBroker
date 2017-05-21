using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Lykke.Domain.Prices;
using Lykke.Domain.Prices.Contracts;
using Lykke.Domain.Prices.Repositories;

namespace CandlesWriter.Core.Tests.Stub
{
    public class CandleHistoryRepositoryStub : ICandleHistoryRepository
    {
        private List<StoreItem> storage = new List<StoreItem>();

        public IReadOnlyList<StoreItem> Stored { get { return this.storage; } }

        public Task<IFeedCandle> GetCandleAsync(string asset, TimeInterval interval, PriceType priceType, DateTime dateTime)
        {
            throw new NotImplementedException();
        }

        public Task<IEnumerable<IFeedCandle>> GetCandlesAsync(string asset, TimeInterval interval, PriceType priceType, DateTime from, DateTime to)
        {
            throw new NotImplementedException();
        }

        public async Task InsertOrMergeAsync(IEnumerable<IFeedCandle> candles, string asset, TimeInterval interval, PriceType priceType)
        {
            foreach(var candle in candles)
            {
                await this.InsertOrMergeAsync(candle, asset, interval, priceType);
            }
        }

        public Task InsertOrMergeAsync(IFeedCandle candle, string asset, TimeInterval interval, PriceType priceType)
        {
            this.storage.Add(new StoreItem()
            {
                candle = candle,
                asset = asset,
                interval = interval,
                priceType = priceType
            });
            return Task.Delay(200);
        }

        public struct StoreItem
        {
            public IFeedCandle candle;
            public string asset;
            public TimeInterval interval;
            public PriceType priceType;
        }
    }
}
