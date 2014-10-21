using Microsoft.WindowsAzure.Storage.Table;
using NBitcoin.Indexer.Internal;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NBitcoin.Indexer
{
    internal abstract class BalanceChangeIndexer<TEntry, TEntity>
        where TEntry : BalanceChangeEntry
        where TEntity : BalanceChangeEntry.Entity
    {
        public abstract CloudTable GetTable();
        protected abstract TEntity CreateQueryEntity(string balanceId);
        protected abstract TEntity CreateEntity(DynamicTableEntity tableEntity);
        protected abstract TEntry CreateEntry(TEntity[] entities);

        public TEntry[] GetBalanceEntries(string balanceId,
                                        IndexerClient indexerClient,
                                        IDictionary<uint256, Transaction> transactionsCache
            )
        {
            var table = GetTable();
            var queryEntity = CreateQueryEntity(balanceId);

            var query = new TableQuery()
                        .Where(
                        TableQuery.CombineFilters(
                                            TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, queryEntity.PartitionKey),
                                            TableOperators.And,
                                            TableQuery.CombineFilters(
                                                TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.GreaterThan, queryEntity.BalanceId + "-"),
                                                TableOperators.And,
                                                TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.LessThan, queryEntity.BalanceId + "|")
                                            )
                        ));

            var entitiesByTransactionId = table
                                    .ExecuteQuery(query)
                                    .Select(CreateEntity)
                                    .GroupBy(e => e.TransactionId);
            List<TEntry> result = new List<TEntry>();
            foreach (var entities in entitiesByTransactionId)
            {
                var entity = entities.Where(e => e.IsLoaded).FirstOrDefault();
                if (entity == null)
                    entity = entities.First();
                if (!entity.IsLoaded)
                    if (LoadBalanceChangeEntity(entity, indexerClient, transactionsCache))
                    {
                        table.Execute(TableOperation.Merge(entity.CreateTableEntity()));
                    }
                var entry = CreateEntry(entities.ToArray());
                result.Add(entry);
            }
            return result.ToArray();
        }

        public bool LoadBalanceChangeEntity(TEntity entity,
            IndexerClient client,
            IDictionary<uint256, Transaction> transactionsCache)
        {
            if (transactionsCache == null)
                transactionsCache = new Dictionary<uint256, Transaction>();
            var txId = new uint256(entity.TransactionId);

            Transaction tx = null;
            if (!transactionsCache.TryGetValue(txId, out tx))
            {
                var txIndexed = client.GetTransaction(false, txId);
                if (txIndexed != null)
                    tx = txIndexed.Transaction;
            }
            if (tx == null)
                return false;

            Money total = Money.Zero;

            if (entity.ReceivedTxOuts.Count == 0)
                entity.ReceivedTxOuts.AddRange(tx.Outputs.Where((o, i) => entity.ReceivedTxOutIndices.Contains((uint)i)).ToList());


            transactionsCache.AddOrReplace(txId, tx);

            foreach (var prev in entity.SpentOutpoints)
            {
                Transaction sourceTransaction = null;
                if (!transactionsCache.TryGetValue(prev.Hash, out sourceTransaction))
                {
                    var sourceIndexedTx = client.GetTransactions(false, new uint256[] { prev.Hash }).FirstOrDefault();
                    if (sourceIndexedTx != null)
                    {
                        sourceTransaction = sourceIndexedTx.Transaction;
                        transactionsCache.AddOrReplace(prev.Hash, sourceTransaction);
                    }
                }
                if (sourceTransaction == null || sourceTransaction.Outputs.Count <= prev.N)
                {
                    return false;
                }
                entity.SpentTxOuts.Add(sourceTransaction.Outputs[(int)prev.N]);
            }

            return true;
        }

        public abstract Dictionary<string, TEntity> ExtractFromTransaction(uint256 blockId, Transaction tx, uint256 txId);
    }
}
