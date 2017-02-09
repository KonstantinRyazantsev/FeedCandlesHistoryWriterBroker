using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;
using Lykke.Domain.Prices;
using CandlesWriter.Core.Tests.Stub;
using System.Threading;
using Lykke.Domain.Prices.Model;

namespace CandlesWriter.Core.Tests
{
    public partial class CandleGenerationControllerTests
    {
        [Fact]
        public void NoExceptionExpectedOnMultithread()
        {
            var logger = new LoggerStub();
            var repo = new CandleHistoryRepositoryStub();
            var controller = new CandleGenerationController(repo, logger, "test component");

            // Cancel after 1 sec
            CancellationTokenSource tokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(1));
            CancellationToken token = tokenSource.Token;

            int consumeCalledTimes = 0;
            int tickCalledTimes = 0;

            // Call Consume and Tick methods at the same time repeatedly and check that there was no exception.
            Task producing = Task.Run(async () =>
            {
                while(!token.IsCancellationRequested)
                {
                    await controller.ConsumeQuote(new Quote() {
                         AssetPair = "btcusd",
                         IsBuy = true,
                         Price = 100,
                         Timestamp = DateTime.Now
                    });
                    Interlocked.Increment(ref consumeCalledTimes);
                }
            }, token);

            Task timer = Task.Run(async () =>
            {
                while (!token.IsCancellationRequested)
                {
                    await controller.Tick();
                    Interlocked.Increment(ref tickCalledTimes);
                }
            }, token);

            Task.WaitAll(producing, timer);

            Assert.True(consumeCalledTimes > 0);
            Assert.True(tickCalledTimes > 0);
        }
    }
}
