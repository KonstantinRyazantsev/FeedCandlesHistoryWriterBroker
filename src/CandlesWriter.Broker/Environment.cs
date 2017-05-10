using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using CandlesWriter.Core;
using Common.Log;

namespace CandlesWriter.Broker
{
    /// <summary>
    /// Application global settings
    /// </summary>
    public class Environment : IEnvironment
    {
        private const int DEFAULT_PRECISION = 5;
        private readonly IAssetPairsRepository repo;
        private readonly ILog log;
        private readonly string componentName;
        private List<IAssetPair> pairs = new List<IAssetPair>();
        private DateTime lastUpdate = DateTime.MinValue;

        public Environment(IAssetPairsRepository repo, ILog log, string componentName)
        {
            this.log = log;
            this.repo = repo;
            this.componentName = componentName;
        }

        public async Task<int> GetPrecision(string asset)
        {
            // Try to find asset
            var assetPair = this.pairs.Where(pair => string.Compare(pair.Id, asset, true) == 0).FirstOrDefault();
            if (assetPair == null) {
                await UpdateDictionary();
                // Try read one more time
                assetPair = this.pairs.Where(pair => string.Compare(pair.Id, asset, true) == 0).FirstOrDefault();

                if (assetPair == null)
                {
                    var message = string.Format("Cannot get accuracy for asset={0}. Dictionary does not contain asset.", asset);
                    await Utils.ThrottleActionAsync(message, async () => 
                        await this.log.WriteErrorAsync(this.componentName, "", "", new InvalidOperationException(message)));
                }
            }
            return assetPair != null ? assetPair.Accuracy : DEFAULT_PRECISION;
        }

        private async Task UpdateDictionary()
        {
            // Do not update often
            if (DateTime.Now - this.lastUpdate > TimeSpan.FromHours(1))
            {
                var dict = await this.repo.GetAllAsync();
                this.pairs = dict != null ? dict.Where(item => !item.IsDisabled).ToList() : new List<IAssetPair>();
                this.lastUpdate = DateTime.Now;
            }
        }
    }
}
