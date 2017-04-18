using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Autofac;

using Common;
using Common.Abstractions;
using Common.Log;
using Lykke.Domain.Prices.Repositories;
using Lykke.Domain.Prices.Model;
using Lykke.RabbitMqBroker.Subscriber;

using CandlesWriter.Broker.Serialization;
using CandlesWriter.Core;

namespace CandlesWriter.Broker
{
    public class Broker: TimerPeriod, IPersistent
    {
        private readonly static string COMPONENT_NAME = "FeedCandlesHistoryWriterBroker";

        private RabbitMqSubscriber<Quote> subscriber;
        private CandleGenerationController controller;
        private ILog logger;

        private ILifetimeScope scope;
        public ILifetimeScope Scope
        {
            get
            {
                return this.scope;
            }
            set
            {
                this.scope = value;
                this.controller.Scope = scope;
            }
        }

        public Broker(
            RabbitMqSubscriber<Quote> subscriber,
            ILog logger)
            : base("BrokerCandlesWriter", (int)TimeSpan.FromMinutes(1).TotalMilliseconds, logger)
        {
            this.logger = logger;
            this.subscriber = subscriber;

            // Using default message reader strategy
            subscriber
                  .SetMessageDeserializer(new MessageDeserializer())
                  .Subscribe(HandleMessage)
                  .SetLogger(logger);

            this.controller = new CandleGenerationController(logger, COMPONENT_NAME);
        }

        private async Task HandleMessage(Quote quote)
        {
            if (quote != null)
            {
                await this.controller.ConsumeQuote(quote);
            }
            else
            {
                await this.logger.WriteWarningAsync(COMPONENT_NAME, string.Empty, string.Empty, "Received quote <NULL>.");
            }
        }

        public override async Task Execute()
        {
            await this.controller.Tick();
        }

        public async Task Save()
        {
            // Persist all remaining intervals
            await this.controller.Tick();
        }
    }
}
