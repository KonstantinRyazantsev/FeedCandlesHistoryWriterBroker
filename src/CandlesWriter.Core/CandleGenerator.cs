using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Lykke.Domain.Prices;
using Lykke.Domain.Prices.Contracts;
using Lykke.Domain.Prices.Model;

namespace CandlesWriter.Core
{
    public sealed class CandleGenerator
    {
        public IEnumerable<IFeedCandle> Generate(IEnumerable<Quote> quotes, TimeInterval interval)
        {
            List<IFeedCandle> result = new List<IFeedCandle>();

            if (quotes != null && quotes.Any())
            {
                var buyQuotes = quotes.Where(q => q.IsBuy);
                var sellQuotes = quotes.Where(q => !q.IsBuy);

                result.AddRange(ConvertToCandles(buyQuotes, interval));
                result.AddRange(ConvertToCandles(sellQuotes, interval));
            }

            return result;
        }

        private IEnumerable<IFeedCandle> ConvertToCandles(IEnumerable<Quote> quotes, TimeInterval interval)
        {
            IEnumerable<IFeedCandle> candles = 
                quotes
                .GroupBy(quote => quote.Timestamp.RoundTo(interval))
                .Select(group => group.ToCandle(group.Key));
            return candles;
        }
    }
}
