using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using Common;
using Common.Abstractions;
using Common.Log;
using Lykke.Domain.Prices.Contracts;
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

        public Broker(
            RabbitMqSubscriber<Quote> subscriber,
            ICandleHistoryRepository repo,
            ILog logger)
            : base("BrokerCandlesWriter", (int)TimeSpan.FromMinutes(1).TotalMilliseconds, logger)
        {
            this.logger = logger;
            this.subscriber = subscriber;

            subscriber
                  .SetMessageDeserializer(new MessageDeserializer())
                  .SetMessageReadStrategy(new MessageReadWithTemporaryQueueStrategy())
                  .Subscribe(HandleMessage)
                  .SetLogger(logger);

            this.controller = new CandleGenerationController(repo, logger, COMPONENT_NAME);
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
