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
        public IEnumerable<IFeedCandle> Generate(IEnumerable<QuoteExt> quotes, TimeInterval interval, PriceType priceType)
        {
            List<IFeedCandle> result = new List<IFeedCandle>();

            if (quotes != null && quotes.Any())
            {
                result.AddRange(ConvertToCandles(quotes, interval, priceType));
            }

            return result;
        }

        private IEnumerable<IFeedCandle> ConvertToCandles(IEnumerable<QuoteExt> quotes, TimeInterval interval, PriceType priceType)
        {
            IEnumerable<Quote> filtered;
            switch (priceType)
            {
                case PriceType.Ask:
                    filtered = quotes.Where(q => q.PriceType == PriceType.Ask); //.Where(q => !q.IsBuy); // sell quotes
                    break;
                case PriceType.Bid:
                    filtered = quotes.Where(q => q.PriceType == PriceType.Bid); //.Where(q => q.IsBuy); // buy quotes
                    break;
                case PriceType.Mid:
                    filtered = quotes.Where(q => q.PriceType == PriceType.Mid);
                    break;
                default:
                    throw new InvalidOperationException(string.Format("Unexpected price type value '{0}'", priceType));
            }

            var list = filtered.ToList();

            IEnumerable<IFeedCandle> candles = filtered
                .GroupBy(quote => quote.Timestamp.RoundTo(interval))
                .Select(group => group.ToCandle(group.Key));

            var res = candles.ToList();

            return candles;
        }
    }
}
