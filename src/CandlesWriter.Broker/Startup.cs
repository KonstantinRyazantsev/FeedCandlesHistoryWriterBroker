using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Autofac;
using Newtonsoft.Json;

using AzureStorage.Tables;
using AzureRepositories.Candles;
using CandlesWriter.Core;
using Common;
using Common.Abstractions;
using Common.Log;
using Lykke.Domain.Prices.Model;
using Lykke.RabbitMqBroker.Subscriber;
using Lykke.Domain.Prices.Repositories;
using CandlesWriter.Broker.Repositories;

namespace CandlesWriter.Broker
{
    internal class Startup
    {
        private AppSettings settings;
        private Broker broker = null;
        private CandleGenerationController controller;
        private QueueMonitor monitor;

        public static string ApplicationName { get { return "FeedCandlesHistoryWriterBroker"; } }

        public Startup(AppSettings settings, ILog log)
        {
            this.settings = settings;
        }

        public void ConfigureServices(ContainerBuilder builder, ILog log)
        {
            var mq = settings.FeedCandlesHistoryWriterBroker.RabbitMq;
            RabbitMqSubscriberSettings subscriberSettings = new RabbitMqSubscriberSettings()
            {
                ConnectionString = $"amqp://{mq.Username}:{mq.Password}@{mq.Host}:{mq.Port}",
                QueueName = mq.QuoteFeed + ".candleshistorywriter",
                ExchangeName = mq.QuoteFeed,
                IsDurable = true
            };

            var dictRepo = new AssetPairsRepository(new AzureTableStorage<AssetPairEntity>(
                settings.FeedCandlesHistoryWriterBroker.ConnectionStrings.DictsConnectionString,
                settings.FeedCandlesHistoryWriterBroker.DictionaryTableName,
                log));

            var env = new Environment(dictRepo, log, ApplicationName);
            var subscriber = new RabbitMqSubscriber<Quote>(subscriberSettings);
            this.controller = new CandleGenerationController(log, ApplicationName, env);
            this.monitor = new QueueMonitor(log, this.controller, settings.FeedCandlesHistoryWriterBroker.QueueWarningSize, ApplicationName);
            this.broker = new Broker(subscriber, log, this.controller, ApplicationName);

            builder.Register(c => new CandleHistoryRepositoryResolver((asset, tableName) => {
                string connString;
                if (!settings.CandleHistoryAssetConnections.TryGetValue(asset, out connString) 
                    || string.IsNullOrEmpty(connString))
                {
                    throw new AppSettingException(string.Format("Connection string for asset pair '{0}' is not specified.", asset));
                }

                var storage = new AzureTableStorage<CandleTableEntity>(connString, tableName, log);
                // Preload table info
                var res = storage.GetDataAsync("ask", "1900-01-01").Result;
                return storage;
            })).As<ICandleHistoryRepository>();

            builder.RegisterInstance(subscriber)
                .As<IStartable>()
                .As<IStopable>();

            builder.RegisterInstance(this.controller)
                .As<IStartable>()
                .As<IStopable>();

            builder.RegisterInstance(this.monitor)
                .As<IStartable>()
                .As<IStopable>();

            builder.RegisterInstance(this.broker)
                .As<IStartable>()
                .As<IStopable>()
                .As<IPersistent>();
        }

        public void Configure(ILifetimeScope scope)
        {
            this.broker.Scope = scope;
        }
    }
}
