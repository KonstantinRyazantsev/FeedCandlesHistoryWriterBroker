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

        public Task<IAssetPair> GetAssetPair(string asset)
        {
            return Task.FromResult(this.assets.Where(a => a.Id == asset).FirstOrDefault() as IAssetPair);
        }
    }
}
