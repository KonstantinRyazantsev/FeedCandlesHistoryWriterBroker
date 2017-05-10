using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using Microsoft.WindowsAzure.Storage.Table;
using CandlesWriter.Core;
using AzureStorage;

namespace CandlesWriter.Broker.Repositories
{
    public class AssetPairEntity : TableEntity, IAssetPair
    {
        public static string GeneratePartitionKey()
        {
            return "AssetPair";
        }

        public static string GenerateRowKey(string id)
        {
            return id;
        }

        public string Id => RowKey;
        public string Name { get; set; }

        public string BaseAssetId { get; set; }
        public string QuotingAssetId { get; set; }
        public int Accuracy { get; set; }
        public int InvertedAccuracy { get; set; }
        public string Source { get; set; }
        public string Source2 { get; set; }
        public bool IsDisabled { get; set; }


        public static AssetPairEntity Create(IAssetPair src)
        {
            return new AssetPairEntity
            {
                PartitionKey = GeneratePartitionKey(),
                RowKey = GenerateRowKey(src.Id),
                Accuracy = src.Accuracy,
                BaseAssetId = src.BaseAssetId,
                QuotingAssetId = src.QuotingAssetId,
                Name = src.Name,
                Source = src.Source,
                Source2 = src.Source2,
                IsDisabled = src.IsDisabled,
                InvertedAccuracy = src.InvertedAccuracy
            };
        }
    }

    public class AssetPairsRepository : IAssetPairsRepository
    {
        private readonly INoSQLTableStorage<AssetPairEntity> _tableStorage;

        public AssetPairsRepository(INoSQLTableStorage<AssetPairEntity> tableStorage)
        {
            _tableStorage = tableStorage;
        }

        public async Task<IEnumerable<IAssetPair>> GetAllAsync()
        {
            var partitionKey = AssetPairEntity.GeneratePartitionKey();
            return await _tableStorage.GetDataAsync(partitionKey);
        }

        public async Task<IAssetPair> GetAsync(string id)
        {
            var partitionKey = AssetPairEntity.GeneratePartitionKey();
            var rowKey = AssetPairEntity.GenerateRowKey(id);
            return await _tableStorage.GetDataAsync(partitionKey, rowKey);
        }
    }
}
