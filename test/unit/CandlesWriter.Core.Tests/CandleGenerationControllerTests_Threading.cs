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
            var candlesRepo = new CandleHistoryRepositoryStub();
            var env = new EnvironmentStub(new List<AssetPair>()
            {
                new AssetPair() { Id="btcusd", Accuracy=5 }
            });
            var controller = new CandleGenerationController(candlesRepo, logger, "test component", env);

            // Cancel after 1 sec
            CancellationTokenSource tokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(2));
            CancellationToken token = tokenSource.Token;

            int consumeCalledTimes = 0;
            int tickCalledTimes = 0;

            // Call Consume and Tick methods at the same time repeatedly and check that there was no exception.
            Task producing = Task.Run(async () =>
            {
                while (!token.IsCancellationRequested)
                {
                    await controller.HandleQuote(new Quote()
                    {
                        AssetPair = "btcusd",
                        IsBuy = true,
                        Price = 100,
                        Timestamp = DateTime.UtcNow
                    });
                    await Task.Delay(50);
                    Interlocked.Increment(ref consumeCalledTimes);
                }
            }, token);

            Task timer = Task.Run(() =>
            {
                while (!token.IsCancellationRequested)
                {
                    controller.Tick();
                    Interlocked.Increment(ref tickCalledTimes);
                }
            }, token);

            Task.WaitAll(producing, timer);

            Assert.True(consumeCalledTimes > 0);
            Assert.True(tickCalledTimes > 0);
            Assert.True(candlesRepo.Stored.Count > 0);
        }
    }
}
