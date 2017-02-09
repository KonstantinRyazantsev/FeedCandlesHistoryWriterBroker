﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Lykke.AzureQueueIntegration;

namespace CandlesWriter.Broker
{
    internal class AppSettings
    {
        public RabbitMqSettings RabbitMq { get; set; } = new RabbitMqSettings();
        public QuotesCandlesHistorySettings QuotesCandlesHistory { get; set; } = new QuotesCandlesHistorySettings();
        public SlackNotificationsSettings SlackNotifications { get; set; } = new SlackNotificationsSettings();

        public class RabbitMqSettings
        {
            public string Host { get; set; }
            public int Port { get; set; }
            public string Username { get; set; }
            public string Password { get; set; }
            public string ExchangeOrderbook { get; set; }
            [JsonProperty("QuteFeed")]
            public string QuoteFeed { get; set; }
        }

        public class QuotesCandlesHistorySettings
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