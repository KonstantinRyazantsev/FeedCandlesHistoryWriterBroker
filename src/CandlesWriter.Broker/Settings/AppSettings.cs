using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Lykke.AzureQueueIntegration;

namespace CandlesWriter.Broker
{
    internal class AppSettings
    {
        public SlackNotificationsSettings SlackNotifications { get; set; } = new SlackNotificationsSettings();
        public FeedCandlesHistoryWriterBrokerSettings FeedCandlesHistoryWriterBroker { get; set; } = new FeedCandlesHistoryWriterBrokerSettings();

        public class FeedCandlesHistoryWriterBrokerSettings
        {
            public RabbitMqSettings RabbitMq { get; set; } = new RabbitMqSettings();
            public ConnectionStringsSettings ConnectionStrings { get; set; } = new ConnectionStringsSettings();
        }

        public class RabbitMqSettings
        {
            public string Host { get; set; }
            public int Port { get; set; }
            public string Username { get; set; }
            public string Password { get; set; }
            public string ExchangeOrderbook { get; set; }
            public string QuoteFeed { get; set; }
        }

        public class ConnectionStringsSettings
        {
            public string HistoryConnectionString { get; set; }
            public string LogsConnectionString { get; set; }
        }

        public class SlackNotificationsSettings
        {
            public AzureQueueSettings AzureQueue { get; set; } = new AzureQueueSettings();
        }
    }
}
