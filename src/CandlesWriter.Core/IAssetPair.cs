using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace CandlesWriter.Core
{
    public interface IAssetPair
    {
        string Id { get; }
        string Name { get; }
        string BaseAssetId { get; }
        string QuotingAssetId { get; }
        int Accuracy { get; }
        int InvertedAccuracy { get; }
        string Source { get; }
        string Source2 { get; }
        bool IsDisabled { get; }
    }

    public class ApiAssetPair
    {
        public string Id { get; set; }
        public string Name { get; set; }
        public int Accuracy { get; set; }
        public int InvertedAccuracy { get; set; }
        public string BaseAssetId { get; set; }
        public string QuotingAssetId { get; set; }
    }

    public class AssetPair : IAssetPair
    {
        public string Id { get; set; }
        public string Name { get; set; }
        public string BaseAssetId { get; set; }
        public string QuotingAssetId { get; set; }
        public int Accuracy { get; set; }
        public int InvertedAccuracy { get; set; }
        public string Source { get; set; }
        public string Source2 { get; set; }
        public bool IsDisabled { get; set; }
    }
}
