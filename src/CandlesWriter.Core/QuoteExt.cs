using Lykke.Domain.Prices;
using Lykke.Domain.Prices.Model;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace CandlesWriter.Core
{
    /// <summary>
    /// Extended quote
    /// </summary>
    public class QuoteExt : Quote
    {
        public PriceType PriceType { get; set; }

        public QuoteExt Clone()
        {
            return new QuoteExt()
            {
                AssetPair = this.AssetPair,
                IsBuy = this.IsBuy,
                Price = this.Price,
                Timestamp = this.Timestamp,
                PriceType = this.PriceType
            };
        }
    }
}
