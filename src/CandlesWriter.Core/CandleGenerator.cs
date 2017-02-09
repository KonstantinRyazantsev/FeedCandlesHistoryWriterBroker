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
        public IEnumerable<ICandle> Generate(IEnumerable<Quote> quotes, TimeInterval interval)
        {
            List<ICandle> result = new List<ICandle>();

            if (quotes != null && quotes.Any())
            {
                var buyQuotes = quotes.Where(q => q.IsBuy);
                var sellQuotes = quotes.Where(q => !q.IsBuy);

                result.AddRange(ConvertToCandles(buyQuotes, interval));
                result.AddRange(ConvertToCandles(sellQuotes, interval));
            }

            return result;
        }

        private IEnumerable<ICandle> ConvertToCandles(IEnumerable<Quote> quotes, TimeInterval interval)
        {
            IEnumerable<ICandle> candles = 
                quotes
                .GroupBy(quote => quote.Timestamp.RoundTo(interval))
                .Select(group => group.ToCandle(group.Key));
            return candles;
        }
    }
}
