using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace CandlesWriter.Core
{
    public interface IAssetPairsRepository
    {
        Task<IEnumerable<IAssetPair>> GetAllAsync();
        Task<IAssetPair> GetAsync(string id);
    }
}
