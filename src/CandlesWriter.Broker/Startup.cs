using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Autofac;
using Newtonsoft.Json;

using AzureStorage.Tables;
using Common;
using Common.Abstractions;
using Common.Log;
using Lykke.Domain.Prices.Model;
using Lykke.Domain.Prices.AzureProvider.History;
using Lykke.Domain.Prices.AzureProvider.History.Model;
using Lykke.RabbitMqBroker;
using Lykke.RabbitMqBroker.Subscriber;

namespace CandlesWriter.Broker
{
    internal class Startup
    {
        private AppSettings settings;

        public static string ApplicationName { get { return "FeedCandlesHistoryWriterBroker"; } }

        public Startup(string settingsJson, ILog log)
        {
            this.settings = JsonConvert.DeserializeObject<AppSettings>(settingsJson);
        }

        public void ConfigureServices(ContainerBuilder builder, ILog log)
        {
            var mq = settings.RabbitMq;
            RabbitMqSettings subscriberSettings = new RabbitMqSettings()
            {
                ConnectionString = $"amqp://{mq.Username}:{mq.Password}@{mq.Host}:{mq.Port}",
                QueueName = mq.QuoteFeed
            };

            var subscriber = new RabbitMqSubscriber<Quote>(subscriberSettings);
            var repo = new CandleHistoryRepository(
                new AzureTableStorage<CandleTableEntity>(
                    settings.FeedCandlesHistoryWriterBroker.ConnectionStrings.HistoryConnectionString, //"UseDevelopmentStorage=true;"
                    "CandlesHistory", log));

            var broker = new Broker(subscriber, repo, log);

            builder.RegisterInstance(subscriber)
                .As<IStartable>()
                .As<IStopable>();

            builder.RegisterInstance(broker)
                .As<IStartable>()
                .As<IStopable>()
                .As<IPersistent>();
        }

        public void Configure(ILifetimeScope scope)
        {
        }
    }
}
