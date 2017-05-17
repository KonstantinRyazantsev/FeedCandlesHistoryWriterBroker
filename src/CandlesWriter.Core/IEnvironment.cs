using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace CandlesWriter.Core
{
    /// <summary>
    /// Application global settings
    /// </summary>
    public interface IEnvironment
    {
        Task<IAssetPair> GetAssetPair(string asset);
    }
}
