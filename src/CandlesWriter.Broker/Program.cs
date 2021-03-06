﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Flurl.Http;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

using AzureStorage.Tables;
using Common.Application;
using Common.Log;
using Lykke.Logs;
using Lykke.SlackNotification.AzureQueue;

namespace CandlesWriter.Broker
{
    public class Program
    {
        public static void Main(string[] args)
        {
            // Initialize logger
            var consoleLog = new LogToConsole();
            var logAggregate = new LogAggregate()
                .AddLogger(consoleLog);
            var log = logAggregate.CreateLogger();

            try
            {
                log.Info("Reading application settings.");
                var config = new ConfigurationBuilder()
                    //.AddJsonFile("appsettings.json", optional: true)
                    .AddEnvironmentVariables()
                    .Build();

                var settingsUrl = config.GetValue<string>("BROKER_SETTINGS_URL");

                log.Info("Loading app settings from web-site.");
                var appSettings = LoadSettings(settingsUrl);

                log.Info("Initializing azure/slack logger.");
                var services = new ServiceCollection(); // only used for azure logger
                logAggregate.ConfigureAzureLogger(services, Startup.ApplicationName, appSettings);

                log = logAggregate.CreateLogger();

                // After log is configured
                //
                log.Info("Creating Startup.");
                var startup = new Startup(appSettings, log);

                log.Info("Configure startup services.");
                startup.ConfigureServices(Application.Instance.ContainerBuilder, log);

                log.Info("Starting application.");
                var scope = Application.Instance.Start();

                log.Info("Configure startup.");
                startup.Configure(scope);

                log.Info("Running application.");
                Application.Instance.Run();

                log.Info("Exit application.");
            }
            catch (Exception ex)
            {
                log.WriteErrorAsync("Program", string.Empty, string.Empty, ex).Wait();
            }
        }

        private static AppSettings LoadSettings(string url)
        {
            var settings = url.GetJsonAsync<AppSettings>().Result;
            // Ignore case for asset keys:
            var origConnections = settings.CandleHistoryAssetConnections;
            settings.CandleHistoryAssetConnections = 
                new Dictionary<string, string>(origConnections, StringComparer.OrdinalIgnoreCase);
            return settings;
        }
    }

    internal static class LogExtensions
    {
        public static void ConfigureAzureLogger(this LogAggregate logAggregate, IServiceCollection services, string appName, AppSettings appSettings)
        {
            var log = logAggregate.CreateLogger();
            var slackSender = services.UseSlackNotificationsSenderViaAzureQueue(appSettings.SlackNotifications.AzureQueue, log);
            var azureLog = new LykkeLogToAzureStorage(appName,
                new AzureTableStorage<LogEntity>(appSettings.FeedCandlesHistoryWriterBroker.ConnectionStrings.LogsConnectionString, appName + "Logs", log),
                slackSender);
            logAggregate.AddLogger(azureLog);
        }

        public static void Info(this ILog log, string info)
        {
            log.WriteInfoAsync("FeedCandlesHistoryWriterBroker", "Program", string.Empty, info).Wait();
        }
    }
}
