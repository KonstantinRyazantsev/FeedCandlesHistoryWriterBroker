using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace CandlesWriter.Core.Tests.Stub
{
    public class EnvironmentStub : IEnvironment
    {
        private IEnumerable<AssetPair> assets;

        public EnvironmentStub(IEnumerable<AssetPair> assets)
        {
            this.assets = assets;
        }

        public async Task<int> GetPrecision(string asset)
        {
            var assetPair = this.assets.Where(a => a.Id == asset).FirstOrDefault();
            return assetPair != null ? assetPair.Accuracy : 5;
        }
    }
}
