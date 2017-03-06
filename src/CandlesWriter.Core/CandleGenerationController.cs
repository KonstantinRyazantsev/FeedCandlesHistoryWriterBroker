using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Autofac;
using Common.Log;
using Lykke.Domain.Prices;
using Lykke.Domain.Prices.Model;
using Lykke.Domain.Prices.Contracts;
using Lykke.Domain.Prices.Repositories;
using Common;

namespace CandlesWriter.Core
{
    public class CandleGenerationController
    {
        private delegate ICandleHistoryRepository RepoProvider();
        private readonly static string PROCESS = "CandleGenerationController";
        private const int CHUNK_SIZE = 10;
        private readonly static IReadOnlyList<TimeInterval> REQUIRED_INTERVALS = new TimeInterval[]
        {
            TimeInterval.Sec,
            TimeInterval.Minute,
            TimeInterval.Min5,
            TimeInterval.Min15,
            TimeInterval.Min30,
            TimeInterval.Hour,
            TimeInterval.Hour4,
            TimeInterval.Hour6,
            TimeInterval.Hour12,
            TimeInterval.Day,
            TimeInterval.Week,
            TimeInterval.Month
        };

        private readonly ICandleHistoryRepository candleRepository;
        private readonly ILog logger;
        private readonly string componentName;
        private readonly ConcurrentQueue<Quote> quotesQueue = new ConcurrentQueue<Quote>();

        /// <summary>Last time service logs were made</summary>
        private DateTime lastServiceLogTime = DateTime.MinValue;
        /// <summary>Average write duration</summary>
        private TimeSpan avgWriteSpan = TimeSpan.Zero;

        private ILifetimeScope globalScope = null;
        /// <summary>
        /// Global scope to resolve dependencies (ICandleHistoryRepository).
        /// Must be set before using.
        /// </summary>
        public ILifetimeScope Scope
        {
            get
            {
                return this.globalScope;
            }
            set
            {
                this.globalScope = value;
            }
        }

        /// <summary>
        /// Constructs repository.
        /// ILifetimeScope must be set for resolving ICandleHistoryRepository.
        /// </summary>
        /// <param name="logger"></param>
        /// <param name="componentName"></param>
        public CandleGenerationController(ILog logger, string componentName = "")
        {
            this.logger = logger;
            this.componentName = componentName;
        }

        public CandleGenerationController(ICandleHistoryRepository repo, ILog logger, string componentName = "")
        {
            this.candleRepository = repo;
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
            if (this.globalScope == null && this.candleRepository == null)
            {
                // Global scope is not set yet, but timer is already running
                await this.logger.WriteWarningAsync(this.componentName, PROCESS, string.Empty, string.Format("Global scope is not set."));
                return;
            }

            if (unprocessedQuotes.Count == 0)
            {
                return;
            }

            Stopwatch watch = new Stopwatch();
            watch.Start();

            ILifetimeScope scope = null;
            try {
                // Provide repository instances
                // 
                if (this.globalScope != null) {
                    scope = this.globalScope.BeginLifetimeScope();
                }

                var repoProvider = scope != null ? 
                    new RepoProvider(() => scope.Resolve<ICandleHistoryRepository>()) :
                    new RepoProvider(() => this.candleRepository);
            
                // Group quotes by asset
                var assetGroups = from q in unprocessedQuotes
                                  group q by q.AssetPair into assetGroup
                                  select assetGroup;

                // Write to storage simultaneously with maximum tasks number
                foreach (var collection in assetGroups.ToPieces(CHUNK_SIZE))
                {
                    var tasks = new List<Task>();
                    foreach (var group in collection)
                    {
                        tasks.Add(ProcessQuotesForAsset(repoProvider(), group, group.Key));
                    }
                    await Task.WhenAll(tasks);
                }
            }
            finally
            {
                if (scope != null) { scope.Dispose(); }
            }

            // Update average write time and log service information
            //
            watch.Stop();
            this.avgWriteSpan = new TimeSpan((this.avgWriteSpan.Ticks + watch.Elapsed.Ticks) / 2);
            if (DateTime.UtcNow - this.lastServiceLogTime > TimeSpan.FromMinutes(1))
            {
                await this.logger.WriteInfoAsync(this.componentName, PROCESS, string.Empty, string.Format("Average write time: {0}", this.avgWriteSpan));
                this.lastServiceLogTime = DateTime.UtcNow;
            }
        }

        private async Task ProcessQuotesForAsset(ICandleHistoryRepository repo, IEnumerable<Quote> quotes, string asset)
        {
            CandleGenerator candleGenerator = new CandleGenerator();
            var data = new Dictionary<TimeInterval, IEnumerable<IFeedCandle>>();

            // For each asset and interval generate candles from quotes and write them to storage.
            //
            foreach (var interval in REQUIRED_INTERVALS)
            {
                data.Add(interval, candleGenerator.Generate(quotes, interval));
            }

            await repo.InsertOrMergeAsync(data, asset);
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
