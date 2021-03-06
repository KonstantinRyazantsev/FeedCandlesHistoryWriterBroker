﻿using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Autofac;
using Common;
using Common.Log;
using Lykke.Domain.Prices;
using Lykke.Domain.Prices.Model;
using Lykke.Domain.Prices.Repositories;
using System.Threading;

namespace CandlesWriter.Core
{
    public class CandleGenerationController : ProducerConsumer<Task>, IStartable, IStopable
    {
        private delegate T RepoProvider<T>();
        private readonly static string PROCESS = "CandleGenerationController";
        private readonly static IReadOnlyList<TimeInterval> REQUIRED_INTERVALS = new TimeInterval[]
        {
            // Store only Sec, Minute, Min30, Hour, Day, Week, Month intervals
            TimeInterval.Sec,
            TimeInterval.Minute,
//            TimeInterval.Min5,
//            TimeInterval.Min15,
            TimeInterval.Min30,
            TimeInterval.Hour,
//            TimeInterval.Hour4,
//            TimeInterval.Hour6,
//            TimeInterval.Hour12,
            TimeInterval.Day,
            TimeInterval.Week,
            TimeInterval.Month
        };

        private readonly static IReadOnlyList<PriceType> REQUIRED_TYPES = new PriceType[]
        {
            PriceType.Ask,
            PriceType.Bid,
            PriceType.Mid
        };

        private readonly ICandleHistoryRepository candleRepository;
        private readonly ILog log;
        private readonly string componentName;
        private readonly ConcurrentQueue<QuoteExt> quotesQueue = new ConcurrentQueue<QuoteExt>();
        private readonly QuoteHandler handler;
        private volatile int queueLength = 0;

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
        /// <param name="log"></param>
        /// <param name="componentName"></param>
        public CandleGenerationController(ILog log, string componentName, IEnvironment env)
            : base(componentName, log)
        {
            this.log = log;
            this.componentName = componentName;
            // Build handles chain
            this.handler = new FilterHandler(env, log,
                                new MidHandler(env,
                                    new DefaultHandler(null)));
        }

        public CandleGenerationController(ICandleHistoryRepository candlesRepo, ILog logger, string componentName, IEnvironment env)
            : this(logger, componentName, env)
        {
            this.candleRepository = candlesRepo;
        }

        public new void Start()
        {
            log.WriteInfoAsync(this.componentName, "", "", "Starting controller.").Wait();
            base.Start();
        }

        public new void Stop()
        {
            log.WriteInfoAsync(this.componentName, "", "", "Stopping controller").Wait();
            base.Stop();
        }

        public int QueueLength
        {
            get { return this.queueLength; }
        }

        public async Task HandleQuote(Quote quote)
        {
            // Validate incoming quote
            //
            ICollection<string> validationErrors = this.Validate(quote);
            if (validationErrors.Count > 0)
            {
                foreach (string error in validationErrors)
                {
                    await this.log.WriteErrorAsync(this.componentName, PROCESS, "", new ArgumentException("Received invalid quote. " + error));
                }
                return; // Skipping invalid quotes
            }

            // Add quote to the processing queue
            var quoteExt = new QuoteExt()
            {
                AssetPair = quote.AssetPair,
                IsBuy = quote.IsBuy,
                Price = quote.Price,
                Timestamp = quote.Timestamp,
                PriceType = quote.IsBuy ? PriceType.Bid : PriceType.Ask
            };
            this.quotesQueue.Enqueue(quoteExt);
        }

        public void Tick()
        {
            // Get a snapshot of quotes collection and process it
            //
            int countQuotes = this.quotesQueue.Count();
            List<QuoteExt> unprocessedQuotes = new List<QuoteExt>(countQuotes);

            for (int i = 0; i < countQuotes; i++)
            {
                QuoteExt quote;
                if (this.quotesQueue.TryDequeue(out quote))
                {
                    unprocessedQuotes.Add(quote);
                }
                else
                {
                    break;
                }
            }

            // Add processing task to producer/consumer's queue
            var task = ProcessQuotes(unprocessedQuotes);
            Interlocked.Increment(ref this.queueLength);
            this.Produce(task);
        }

        protected override async Task Consume(Task t)
        {
            // On consume just await task
            //
#if DEBUG
            await this.log.WriteInfoAsync(this.componentName, "", "", "Consuming task. Amount of tasks in queue=" + this.queueLength);
#endif
            try
            {
                await t;
            }
            finally
            {
                Interlocked.Decrement(ref this.queueLength);
            }
#if DEBUG
            await this.log.WriteInfoAsync(this.componentName, "", "", "Task is finished. Amount of tasks in queue=" + this.queueLength);
#endif
        }

        private async Task ProcessQuotes(List<QuoteExt> unprocessedQuotes)
        {
            if (this.globalScope == null && (this.candleRepository == null)) // || this.assetsRepository == null))
            {
                // Global scope is not set yet, but timer is already running
                await this.log.WriteWarningAsync(this.componentName, PROCESS, string.Empty, string.Format("Global scope is not set."));
                return;
            }

            if (unprocessedQuotes.Count == 0)
            {
                return;
            }

            // Sort quotes by time before passing them to process chain
            unprocessedQuotes.Sort((lhs, rhs) => DateTime.Compare(lhs.Timestamp, rhs.Timestamp));

            // Process incoming quotes to temporary queue
            //
            Queue<QuoteExt> output = new Queue<QuoteExt>();
            foreach (var quote in unprocessedQuotes)
            {
                await this.handler.Handle(quote, output);
            }

            // Replace incoming quotes with processed quotes
            unprocessedQuotes = output.ToList();

            // Start generating candles
            Stopwatch watch = new Stopwatch();
            watch.Start();

            ILifetimeScope scope = null;
            try
            {
                // Provide repository instances
                // 
                if (this.globalScope != null)
                {
                    scope = this.globalScope.BeginLifetimeScope();
                }

                var repo = scope != null ? scope.Resolve<ICandleHistoryRepository>() : this.candleRepository;

                // Group quotes by asset
                var assetGroups = from q in unprocessedQuotes
                                  group q by q.AssetPair into assetGroup
                                  select assetGroup;
                var tasks = new List<Task>();
                foreach (var group in assetGroups)
                {
                    tasks.Add(ProcessQuotesForAsset(repo, group, group.Key));
                }

                var all = Task.WhenAll(tasks);
                try
                {
                    await all;
                }
                catch (Exception)
                {
                    foreach (var failedTask in tasks.Where(t => t.Exception != null))
                    {
                        await LogException(failedTask.Exception);
                    }
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
            if (DateTime.UtcNow - this.lastServiceLogTime > TimeSpan.FromHours(1))
            {
                await this.log.WriteInfoAsync(this.componentName, PROCESS, string.Empty, string.Format("Average write time: {0}", this.avgWriteSpan));
                this.lastServiceLogTime = DateTime.UtcNow;
            }
        }

        private async Task ProcessQuotesForAsset(ICandleHistoryRepository repo, IEnumerable<QuoteExt> quotes, string asset)
        {
            if (quotes.Count() != 0)
            {
                // For each asset and interval generate candles from quotes and write them to storage.
                //
                foreach (var interval in REQUIRED_INTERVALS)
                {
                    await this.Insert(repo, quotes, asset, interval);
                }
            }
        }

        private async Task Insert(ICandleHistoryRepository repo, IEnumerable<QuoteExt> quotes, string asset, TimeInterval interval)
        {
            CandleGenerator candleGenerator = new CandleGenerator();

            foreach (var type in REQUIRED_TYPES)
            {
                var data = candleGenerator.Generate(quotes, interval, type);
                await Retry(() =>
                {
                    return repo.InsertOrMergeAsync(data, asset, interval, type);
                });
            }
        }

        private async Task Retry(Func<Task> syncAction)
        {
            int retryDelaySec = 3;
            int retryCount = 3;
            while (retryCount > 0)
            {
                try
                {
                    await syncAction();
                    return;
                }
                catch(AppSettingException)
                {
                    // Do not retry on configuration exceptions
                    throw;
                }
                catch(Exception ex)
                {
                    await log.WriteWarningAsync(this.componentName, "Inserting", "", 
                        $"Retrying insert candles in {retryDelaySec} second after received exception: {ex.Message}");
                    await Task.Delay((int)TimeSpan.FromSeconds(retryDelaySec).TotalMilliseconds);
                    retryCount--;
                }
            }
            await syncAction();
        }

        private async Task LogException(Exception ex)
        {
            // Ignore exceptions for not configured connection strings.
            // Log any other exceptions.

            var aggr = ex as AggregateException;
            if (aggr != null)
            {
                foreach (var innerex in aggr.InnerExceptions)
                {
                    if (innerex is AppSettingException)
                    {
#if DEBUG
                        await this.log.WriteInfoAsync(this.componentName, "", "", "Ignoring incoming asset: " + ex.Message);
#endif
                    }
                    else
                    {
                        await this.log.WriteErrorAsync(this.componentName, "", "", ex);
                    }
                }
            }
            else if (ex is AppSettingException)
            {
#if DEBUG
                await this.log.WriteInfoAsync(this.componentName, "", "", "Ignoring incoming asset: " + ex.Message);
#endif
            }
            else
            {
                await this.log.WriteErrorAsync(this.componentName, "", "", ex);
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
