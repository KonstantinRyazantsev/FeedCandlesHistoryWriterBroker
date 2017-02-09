﻿using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Common.Log;
using Lykke.Domain.Prices;
using Lykke.Domain.Prices.Model;
using Lykke.Domain.Prices.Contracts;

namespace CandlesWriter.Core
{
    public class CandleGenerationController
    {
        private readonly static string PROCESS = "CandleGenerationController";
        private readonly static IReadOnlyList<TimeInterval> REQUIRED_INTERVALS = new TimeInterval[]
        {
            TimeInterval.Sec,
            TimeInterval.Minute,
            TimeInterval.Min5,
            TimeInterval.Min15,
            TimeInterval.Min30,
            TimeInterval.Hour,
            TimeInterval.Day,
            TimeInterval.Month
        };

        private readonly ICandleHistoryRepository candleRepository;
        private readonly ILog logger;
        private readonly string componentName;
        private readonly ConcurrentQueue<Quote> quotesQueue = new ConcurrentQueue<Quote>();

        public CandleGenerationController(ICandleHistoryRepository candleRepository, ILog logger, string componentName = "")
        {
            this.candleRepository = candleRepository;
            this.logger = logger;
            this.componentName = componentName;
        }

        public async Task ConsumeQuote(Quote quote)
        {
            // Validate incoming quote
            //
            ICollection<string> validationErrors = this.Validate(quote);
            if (validationErrors.Count > 0)
            {
                foreach (string error in validationErrors)
                {
                    await this.logger.WriteErrorAsync(this.componentName, PROCESS, "", new ArgumentException("Received invalid quote. " + error));
                }
                return; // Skipping invalid quotes
            }

            // Add quote to the processing queue
            this.quotesQueue.Enqueue(quote);
        }

        public async Task Tick()
        {
            // Get a snapshot of quotes collection and process it
            //
            int countQuotes = this.quotesQueue.Count();
            List<Quote> unprocessedQuotes = new List<Quote>(countQuotes);
            for (int i = 0; i < countQuotes; i++)
            {
                Quote quote;
                if (this.quotesQueue.TryDequeue(out quote))
                {
                    unprocessedQuotes.Add(quote);
                }
                else
                {
                    break;
                }
            }

            await ProcessQuotes(unprocessedQuotes);
        }

        private async Task ProcessQuotes(List<Quote> unprocessedQuotes)
        {
            if (unprocessedQuotes.Count == 0)
            {
                return;
            }

            CandleGenerator candleGenerator = new CandleGenerator();

            // Group quotes by asset
            var assetGroups = from q in unprocessedQuotes
                         group q by q.AssetPair into assetGroup
                         select assetGroup;

            foreach (var assetGroup in assetGroups)
            {
                foreach (var interval in REQUIRED_INTERVALS)
                {
                    // For each asset and interval generate candles from quotes and write them to storage.
                    //
                    IEnumerable<Quote> quotes = assetGroup;
                    IEnumerable<ICandle> candles = candleGenerator.Generate(quotes, interval);

                    // TODO: Write whole collection
                    foreach (var candle in candles)
                    {
                        await this.candleRepository.InsertOrMergeAsync(candle, assetGroup.Key, interval);
                    }
                }
            }
        }

        private ICollection<string> Validate(Quote quote)
        {
            List<string> errors = new List<string>();

            if (quote == null)
            {
                errors.Add("Argument 'Order' is null.");
            }
            if (quote != null && string.IsNullOrEmpty(quote.AssetPair))
            {
                errors.Add(string.Format("Invalid 'AssetPair': '{0}'", quote.AssetPair ?? ""));
            }
            if (quote != null && (quote.Timestamp == DateTime.MinValue || quote.Timestamp == DateTime.MaxValue))
            {
                errors.Add(string.Format("Invalid 'Timestamp' range: '{0}'", quote.Timestamp));
            }
            if (quote != null && quote.Timestamp.Kind != DateTimeKind.Utc)
            {
                errors.Add(string.Format("Invalid 'Timestamp' Kind (UTC is required): '{0}'", quote.Timestamp));
            }

            return errors;
        }
    }
}