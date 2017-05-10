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
            public int QueueWarningSize { get; set; } = 10;
            public string DictionaryTableName { get; set; }
        }

        public class RabbitMqSettings
        {
            public string Host { get; set; }
            public int Port { get; set; }
            public string Username { get; set; }
            public string Password { get; set; }
            public string QuoteFeed { get; set; }
        }

        public class ConnectionStringsSettings
        {
            public string LogsConnectionString { get; set; }
            public string DictsConnectionString { get; set; }
            public IDictionary<string, string> AssetConnections { get; set; } = new Dictionary<string, string>();
        }

        public class SlackNotificationsSettings
        {
            public AzureQueueSettings AzureQueue { get; set; } = new AzureQueueSettings();
        }
    }
}
