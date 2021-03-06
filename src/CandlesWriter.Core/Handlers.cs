﻿using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Lykke.Domain.Prices.Contracts;
using Lykke.Domain.Prices.Model;
using Lykke.Domain.Prices;
using Common.Log;

namespace CandlesWriter.Core
{
    public abstract class QuoteHandler
    {
        private QuoteHandler _next;
        public QuoteHandler(QuoteHandler next)
        {
            _next = next;
        }

        public virtual async Task Handle(QuoteExt quote, Queue<QuoteExt> output)
        {
            if (_next != null)
            {
                await _next.Handle(quote, output);
            }
        }
    }

    /// <summary>
    /// Filters incoming assets based on whether they exist in dictanary or not.
    /// </summary>
    public class FilterHandler : QuoteHandler
    {
        private IEnvironment env;
        private ILog log;

        public FilterHandler(IEnvironment env, ILog log, QuoteHandler next)
            : base(next)
        {
            this.env = env;
            this.log = log;
        }

        public override async Task Handle(QuoteExt quote, Queue<QuoteExt> output)
        {
            // If asset dictionary does not contain asset, then ignore this quote.
            var assetPair = await this.env.GetAssetPair(quote.AssetPair);
            if (assetPair != null)
            {
                await base.Handle(quote, output);
            }
            else
            {
#if DEBUG
                await this.log.WriteInfoAsync("FeedCandlesHistoryWriterBroker", "", "", "Ignoring incoming asset " + quote.AssetPair);
#endif
            }
        }
    }

    /// <summary>
    /// Generates Mid price from incoming Ask/Bid prices.
    /// </summary>
    /// <remarks>Stores last ask/bid values for each asset and generates Mid for every change.</remarks>
    public class MidHandler : QuoteHandler
    {
        private Dictionary<string, QuoteExt> ask = new Dictionary<string, QuoteExt>();
        private Dictionary<string, QuoteExt> bid = new Dictionary<string, QuoteExt>();
        private IEnvironment env;

        public MidHandler(IEnvironment env, QuoteHandler next)
            : base(next)
        {
            this.env = env;
        }

        public override async Task Handle(QuoteExt quote, Queue<QuoteExt> output)
        {
            var assetKey = quote.AssetPair.ToLowerInvariant();
            var copy = quote.Clone();

            // Update current ask/bid values
            if (quote.PriceType == PriceType.Bid)
            {
                QuoteExt latest = null;
                this.bid.TryGetValue(assetKey, out latest);
                this.bid[assetKey] = GetLatest(latest, copy);
            }
            else if (quote.PriceType == PriceType.Ask)
            {
                QuoteExt latest = null;
                this.ask.TryGetValue(assetKey, out latest);
                this.ask[assetKey] = GetLatest(latest, copy);
            }
            else
            {
                await base.Handle(quote, output);
                return;
            }

            // Make Mid quote
            QuoteExt mid = await MakeMiddle(assetKey, quote.AssetPair);

            // Pass further orignial quote and new mid quote.
            if (mid != null)
            {
                await base.Handle(mid, output);
            }
            await base.Handle(quote, output);
        }

        private static QuoteExt GetLatest(QuoteExt lhs, QuoteExt rhs)
        {
            if (lhs != null && rhs != null)
            {
                return lhs.Timestamp > rhs.Timestamp ? lhs : rhs;
            }
            return lhs != null ? lhs : rhs;
        }

        private async Task<QuoteExt> MakeMiddle(string assetKey, string asset)
        {
            QuoteExt currentAsk = null;
            QuoteExt currentBid = null;

            this.ask.TryGetValue(assetKey, out currentAsk);
            this.bid.TryGetValue(assetKey, out currentBid);

            var assetPair = await this.env.GetAssetPair(asset);

            QuoteExt mid = null;
            // If asset dictionary does not contain asset, ignore it.
            if (assetPair != null && currentAsk != null && currentBid != null)
            {
                mid = new QuoteExt()
                {
                    AssetPair = asset,
                    Price = Math.Round((currentAsk.Price + currentBid.Price) / 2, assetPair.Accuracy),
                    // Controller ensures that both dates are in UTC
                    Timestamp = DateTime.Compare(currentAsk.Timestamp, currentBid.Timestamp) >= 0 ? currentAsk.Timestamp : currentBid.Timestamp,
                    PriceType = PriceType.Mid
                };
            }
            return mid;
        }
    }

    /// <summary>
    /// Puts quote to output queue
    /// </summary>
    public class DefaultHandler : QuoteHandler
    {
        public DefaultHandler(QuoteHandler next)
            : base(next)
        {
        }

        public override async Task Handle(QuoteExt quote, Queue<QuoteExt> output)
        {
            output.Enqueue(quote);
            await base.Handle(quote, output);
        }
    }
}
