using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Common;
using Common.Log;
using CandlesWriter.Core;

namespace CandlesWriter.Broker
{
    /// <summary>
    /// Monitors length of tasks queue.
    /// </summary>
    public class QueueMonitor : TimerPeriod, IStopable
    {
        private readonly string componentName;
        private readonly ILog logger;
        private readonly CandleGenerationController controller;
        private readonly int warningSize;

        public QueueMonitor(ILog logger, CandleGenerationController controller, int warningSize, string componentName)
            : base(componentName, (int)TimeSpan.FromMinutes(1).TotalMilliseconds, logger)
        {
            this.componentName = componentName;
            this.logger = logger;
            this.controller = controller;
            this.warningSize = warningSize;
        }

        public override void Start()
        {
            logger.WriteInfoAsync(this.componentName, "", "", "Starting monitor").Wait();
            base.Start();
        }

        public new void Stop()
        {
            logger.WriteInfoAsync(this.componentName, "", "", "Stopping monitor").Wait();
            base.Stop();
        }

        public override async Task Execute()
        {
            var currentLength = this.controller.QueueLength;
            if (currentLength > warningSize)
            {
                await this.logger.WriteWarningAsync(componentName, "QueueMonitor", "", 
                    string.Format("Processing queue's size exceeded warning level ({0}) and now equals {1}.", warningSize, currentLength));
            }
        }
    }
}
