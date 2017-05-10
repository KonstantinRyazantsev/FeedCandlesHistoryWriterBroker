using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Autofac;

using Common;
using Common.Abstractions;
using Common.Log;
using Lykke.Domain.Prices.Model;
using Lykke.RabbitMqBroker.Subscriber;

using CandlesWriter.Broker.Serialization;
using CandlesWriter.Core;

namespace CandlesWriter.Broker
{
    public class Broker: TimerPeriod, IPersistent, IStopable
    {
        private readonly RabbitMqSubscriber<Quote> subscriber;
        private readonly CandleGenerationController controller;
        private readonly ILog logger;
        private readonly string componentName;

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
            ILog logger,
            CandleGenerationController controller,
            string componentName)
            : base(componentName, (int)TimeSpan.FromMinutes(1).TotalMilliseconds, logger)
        {
            this.componentName = componentName;
            this.logger = logger;
            this.controller = controller;
            this.subscriber = subscriber;

            // Using default message reader strategy
            subscriber
                  .SetMessageDeserializer(new MessageDeserializer())
                  .Subscribe(HandleMessage)
                  .SetLogger(logger);
        }

        private async Task HandleMessage(Quote quote)
        {
            if (quote != null)
            {
                await this.controller.HandleQuote(quote);
            }
            else
            {
                await this.logger.WriteWarningAsync(this.componentName, string.Empty, string.Empty, "Received quote <NULL>.");
            }
        }

        public override void Start()
        {
            logger.WriteInfoAsync(this.componentName, "", "", "Starting broker").Wait();
            base.Start();
        }

        public new void Stop()
        {
            logger.WriteInfoAsync(this.componentName, "", "", "Stopping broker").Wait();
            base.Stop();
        }

        public override async Task Execute()
        {
            this.controller.Tick();
            await Task.FromResult(0);
        }

        public async Task Save()
        {
            // Persist all remaining intervals
            this.controller.Tick();
            await Task.FromResult(0);
        }
    }
}
