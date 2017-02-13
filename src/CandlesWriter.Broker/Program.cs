using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Newtonsoft.Json;

using AzureStorage.Tables;
using Common.HttpRemoteRequests;
using Common.Log;
using Lykke.Logs;
using Lykke.RabbitMqBroker;
using Lykke.SlackNotification.AzureQueue;

namespace CandlesWriter.Broker
{
    public class Program
    {
        private static readonly string COMPONENT = "FeedCandlesHistoryWriterBroker";

        public static void Main(string[] args)
        {
            var serviceCollection = new ServiceCollection();
            var consoleLog = new LogToConsole();
            var loggerFanout = new LoggerFanout()
                .AddLogger("console", consoleLog);

            loggerFanout.WriteInfoAsync(COMPONENT, string.Empty, string.Empty, "Loading \"FeedCandlesHistoryWriterBroker\".").Wait();
            loggerFanout.WriteInfoAsync(COMPONENT, string.Empty, string.Empty, "Reading app settings.").Wait();

            var config = new ConfigurationBuilder()
                .AddJsonFile("appsettings.json", optional: false)
                //.AddJsonFile($"appsettings.{env.EnvironmentName}.json", optional: true)
                .Build();

            string settingsUrl = config.GetValue<string>("settingsUrl");

            loggerFanout.WriteInfoAsync(COMPONENT, string.Empty, string.Empty, "Loading app settings from web-site.").Wait();
            // Reading app settings from settings web-site
            HttpRequestClient webClient = new HttpRequestClient();
            string json = webClient.GetRequest(settingsUrl, "application/json").Result;
            var appSettings = JsonConvert.DeserializeObject<AppSettings>(json);

            loggerFanout.WriteInfoAsync(COMPONENT, string.Empty, string.Empty, "Initializing azure/slack logger.").Wait();
            // Initialize slack sender
            var slackSender = serviceCollection.UseSlackNotificationsSenderViaAzureQueue(appSettings.SlackNotifications.AzureQueue, consoleLog);
            // Initialize azure logger
            var azureLog = new LykkeLogToAzureStorage("FeedCandlesHistoryWriterBroker", 
                new AzureTableStorage<LogEntity>(appSettings.QuotesCandlesHistory.LogsConnectionString, "FeedCandlesHistoryWriterBrokerLogs", consoleLog), 
                slackSender);
            loggerFanout.AddLogger("azure", azureLog);

            var mq = appSettings.RabbitMq;
            RabbitMqSettings subscriberSettings = new RabbitMqSettings()
            {
                 ConnectionString = $"amqp://{mq.Username}:{mq.Password}@{mq.Host}:{mq.Port}",
                 QueueName = mq.QuoteFeed
            };

            // Start broker
            loggerFanout.WriteInfoAsync(COMPONENT, string.Empty, string.Empty, "Starting queue subscription.").Wait();
            Broker broker = new Broker(subscriberSettings, "UseDevelopmentStorage=true;", "CandlesHistory", loggerFanout);
            //Broker broker = new Broker(subscriberSettings, appSettings.QuotesCandlesHistory.HistoryConnectionString, "CandlesHistory", loggerFanout);
            broker.Start();

            Console.WriteLine("Press any key...");
            Console.ReadLine();

            loggerFanout.WriteInfoAsync(COMPONENT, string.Empty, string.Empty, "Stopping broker.").Wait();
            broker.Stop();
            loggerFanout.WriteInfoAsync(COMPONENT, string.Empty, string.Empty, "Brokker is stopped.").Wait();
        }
    }
}
