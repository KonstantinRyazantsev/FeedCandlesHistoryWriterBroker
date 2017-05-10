using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;
using CandlesWriter.Core.Tests.Stub;
using System.Threading;
using Lykke.Domain.Prices.Model;

namespace CandlesWriter.Core.Tests
{
    public partial class CandleGenerationControllerTests
    {
        private EnvironmentStub env = new EnvironmentStub(new List<AssetPair>()
        {
            new AssetPair() { Id="btcrub", Accuracy=5 }
        });

        [Fact]
        public void NullIsIgnored()
        {
            var logger = new LoggerStub();
            var repo = new CandleHistoryRepositoryStub();
            var controller = new CandleGenerationController(repo, logger, "test component", env);

            controller.HandleQuote(null).Wait();
            Assert.Equal(0, repo.Stored.Count);
        }

        [Fact]
        public void EmptyAssetIsIgnored()
        {
            var logger = new LoggerStub();
            var repo = new CandleHistoryRepositoryStub();
            var controller = new CandleGenerationController(repo, logger, "test component", env);

            controller.HandleQuote(new Quote() { AssetPair = null, IsBuy = true, Price = 1, Timestamp = Utils.ParseUtc("2017-01-01 10:10:10Z") }).Wait();
            controller.HandleQuote(new Quote() { AssetPair = "", IsBuy = true, Price = 1, Timestamp = Utils.ParseUtc("2017-01-01 10:10:10Z") }).Wait();

            Assert.Equal(0, repo.Stored.Count);
        }

        [Fact]
        public void InvalidDateIsIgnored()
        {
            var logger = new LoggerStub();
            var repo = new CandleHistoryRepositoryStub();
            var controller = new CandleGenerationController(repo, logger, "test component", env);

            DateTime unspecified = new DateTime(2017, 1, 1, 0, 0, 0, DateTimeKind.Unspecified);
            DateTime local = new DateTime(2017, 1, 1, 0, 0, 0, DateTimeKind.Local);

            controller.HandleQuote(new Quote() { AssetPair = "btcrub", IsBuy = true, Price = 1, Timestamp = unspecified }).Wait();
            controller.HandleQuote(new Quote() { AssetPair = "btcrub", IsBuy = true, Price = 1, Timestamp = local }).Wait();
            controller.HandleQuote(new Quote() { AssetPair = "btcrub", IsBuy = true, Price = 1, Timestamp = DateTime.MinValue }).Wait();
            controller.HandleQuote(new Quote() { AssetPair = "btcrub", IsBuy = true, Price = 1, Timestamp = DateTime.MaxValue }).Wait();
            Assert.Equal(0, repo.Stored.Count);
        }
    }
}
