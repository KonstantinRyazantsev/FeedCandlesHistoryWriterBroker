using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using AzureStorage.Tables;
using Common.Log;
using Microsoft.WindowsAzure.Storage.Table;
using System.Diagnostics;

namespace CandlesWriter.Core.IntTests
{
    internal class AzureTableStorageWrapper<T> : AzureTableStorage<T> where T : class, ITableEntity, new()
    {
        private int _calls = 0;
        private readonly int _exceptionFreq;
        private readonly object _sync = new object();

        public AzureTableStorageWrapper(string connectionString, string tableName, ILog log, int exceptionFreq = 0)
            : base(connectionString, tableName, log)
        {
            _exceptionFreq = exceptionFreq;
        }

        public override async Task<T> GetDataAsync(string partition, string row)
        {
            lock (_sync)
            {
                if (_exceptionFreq != 0 && _calls >= _exceptionFreq)
                {
                    throw new InvalidOperationException("Test exception");
                }
                _calls++;
            }

            return await base.GetDataAsync(partition, row);
        }
    }
}
