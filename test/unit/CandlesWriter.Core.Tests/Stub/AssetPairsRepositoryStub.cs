using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace CandlesWriter.Core.Tests.Stub
{
    public class AssetPairsRepositoryStub : IAssetPairsRepository
    {
        private IEnumerable<AssetPair> assets;

        public AssetPairsRepositoryStub(IEnumerable<AssetPair> assets)
        {
            this.assets = assets;
        }

        public async Task<IEnumerable<IAssetPair>> GetAllAsync()
        {
            return await Task.FromResult(this.assets);
        }

        public async Task<IAssetPair> GetAsync(string id)
        {
            return await Task.FromResult(this.assets.Where(a => a.Id == id).FirstOrDefault());
        }
    }
}
