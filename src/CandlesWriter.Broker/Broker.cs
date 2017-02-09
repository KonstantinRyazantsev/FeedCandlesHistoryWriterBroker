using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using AzureStorage.Tables;
using Common;
using Common.Log;
using Lykke.Domain.Prices.Contracts;
using Lykke.Domain.Prices.AzureProvider.History;
using Lykke.Domain.Prices.AzureProvider.History.Model;
using Lykke.Domain.Prices.Model;
using Lykke.RabbitMqBroker;
using Lykke.RabbitMqBroker.Subscriber;

using CandlesWriter.Broker.Serialization;
using CandlesWriter.Core;

namespace CandlesWriter.Broker
{
    public interface IStartable
    {
        void Start();
    }

    public class Broker: TimerPeriod, IStartable, IStopable, IDisposable
    {
        private readonly static string COMPONENT_NAME = "FeedCandlesHistoryWriterBroker";
        private readonly static string PROCESS = "Broker";

        private RabbitMqSubscriber<Quote> subscriber;
        private CandleGenerationController controller;
        private ILog logger;

        private bool isStarted = false;
        private bool isDisposed = false;

        public Broker(
            RabbitMqSettings rabitMqSubscriberSettings,
            string historyTableConnectionString,
            string historyTableName,
            ILog logger)
            : base("BrokerCandlesWriter", (int)TimeSpan.FromMinutes(1).TotalMilliseconds, logger)
        {
            this.subscriber =
                new RabbitMqSubscriber<Quote>(rabitMqSubscriberSettings)
                  .SetMessageDeserializer(new MessageDeserializer())
                  .SetMessageReadStrategy(new MessageReadWithTemporaryQueueStrategy())
                  .Subscribe(HandleMessage)
                  .SetLogger(logger);

            this.logger = logger;

            ICandleHistoryRepository candleRepository = new CandleHistoryRepository(
                new AzureTableStorage<CandleTableEntity>(historyTableConnectionString, historyTableName, logger));

            this.controller = new CandleGenerationController(candleRepository, logger, COMPONENT_NAME);
        }

        public override async Task Execute()
        {
            await this.controller.Tick();
        }

        void IStartable.Start()
        {
            EnsureNotDisposed();
            if (!this.isStarted)
            {
                this.subscriber.Start();
                this.isStarted = true;
            }
        }

        void IStopable.Stop()
        {
            EnsureNotDisposed();
            if (this.isStarted)
            {
                this.subscriber.Stop();
                this.isStarted = false;
            }
        }

        private async Task HandleMessage(Quote quote)
        {
            await this.controller.ConsumeQuote(quote);
        }

        #region "IDisposable implementation"
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }
        ~Broker()
        {
            Dispose(false);
        }

        private void Dispose(bool disposing)
        {
            if (disposing)
            {
                // get rid of managed resources
            }
            // get rid of unmanaged resources
            this.isDisposed = true;
        }

        private void EnsureNotDisposed()
        {
            if (this.isDisposed)
            {
                this.logger.WriteErrorAsync(COMPONENT_NAME, PROCESS, "", new InvalidOperationException("Disposed object Broker has been called"), DateTime.Now);
            }
        }
        #endregion
    }
}
