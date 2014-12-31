using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Table;
using NBitcoin.DataEncoders;
using NBitcoin.Indexer.Converters;
using NBitcoin.Indexer.Internal;
using NBitcoin.OpenAsset;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using NBitcoin;
using System.Threading.Tasks;

namespace NBitcoin.Indexer
{
    public class IndexerClient
    {
        private readonly IndexerConfiguration _Configuration;
        public IndexerConfiguration Configuration
        {
            get
            {
                return _Configuration;
            }
        }

        public IndexerClient(IndexerConfiguration configuration)
        {
            if (configuration == null)
                throw new ArgumentNullException("configuration");
            _Configuration = configuration;
        }


        public Block GetBlock(uint256 blockId)
        {
            var ms = new MemoryStream();
            var container = Configuration.GetBlocksContainer();
            try
            {

                container.GetPageBlobReference(blockId.ToString()).DownloadToStream(ms);
                ms.Position = 0;
                Block b = new Block();
                b.ReadWrite(ms, false);
                return b;
            }
            catch (StorageException ex)
            {
                if (ex.RequestInformation != null && ex.RequestInformation.HttpStatusCode == 404)
                {
                    return null;
                }
                throw;
            }
        }

        public TransactionEntry GetTransaction(bool lazyLoadSpentOutput, uint256 txId)
        {
            return GetTransactions(lazyLoadSpentOutput, new uint256[] { txId }).First();
        }
        public TransactionEntry GetTransaction(uint256 txId)
        {
            return GetTransactions(true, new[] { txId }).First();
        }

        public TransactionEntry[] GetTransactions(bool lazyLoadPreviousOutput, uint256[] txIds)
        {
            return GetTransactions(lazyLoadPreviousOutput, false, txIds);
        }

        public TransactionEntry GetTransaction(bool lazyLoadPreviousOutput, bool fetchColor, uint256 txId)
        {
            return GetTransactions(lazyLoadPreviousOutput, fetchColor, new[] { txId }).First();
        }

        /// <summary>
        /// Get transactions in Azure Table
        /// </summary>
        /// <param name="txIds"></param>
        /// <returns>All transactions (with null entries for unfound transactions)</returns>
        public TransactionEntry[] GetTransactions(bool lazyLoadPreviousOutput, bool fetchColor, uint256[] txIds)
        {
            var result = new TransactionEntry[txIds.Length];
            var queries = new TableQuery[txIds.Length];
            try
            {
                Parallel.For(0, txIds.Length, i =>
                {
                    if (txIds[0] == 0)
                        return;
                    var table = Configuration.GetTransactionTable();
                    var searchedEntity = new TransactionEntry.Entity(txIds[i]);
                    queries[i] = new TableQuery()
                                    .Where(
                                            TableQuery.CombineFilters(
                                                TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, searchedEntity.PartitionKey),
                                                TableOperators.And,
                                                TableQuery.CombineFilters(
                                                    TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.GreaterThan, txIds[i].ToString() + "-"),
                                                    TableOperators.And,
                                                    TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.LessThan, txIds[i].ToString() + "|")
                                                )
                                          ));

                    var entities = table.ExecuteQuery(queries[i])
                                       .Select(e => new TransactionEntry.Entity(e)).ToArray();
                    if (entities.Length == 0)
                        result[i] = null;
                    else
                    {
                        result[i] = new TransactionEntry(entities);
                        if (result[i].Transaction == null)
                        {
                            foreach (var block in result[i].BlockIds.Select(id => GetBlock(id)).Where(b => b != null))
                            {
                                result[i].Transaction = block.Transactions.FirstOrDefault(t => t.GetHash() == txIds[i]);
                                entities[0].Transaction = result[i].Transaction;
                                if (entities[0].Transaction != null)
                                {
                                    table.Execute(TableOperation.Merge(entities[0].CreateTableEntity()));
                                }
                                break;
                            }
                        }

                        if (fetchColor && result[i].ColoredTransaction == null)
                        {
                            result[i].ColoredTransaction = ColoredTransaction.FetchColors(txIds[i], result[i].Transaction, new IndexerColoredTransactionRepository(Configuration));
                            entities[0].ColoredTransaction = result[i].ColoredTransaction;
                            if (entities[0].ColoredTransaction != null)
                            {
                                table.Execute(TableOperation.Merge(entities[0].CreateTableEntity()));
                            }
                        }

                        var needTxOut = result[i].SpentCoins == null && lazyLoadPreviousOutput && result[i].Transaction != null;
                        if (needTxOut)
                        {
                            var tasks =
                                result[i].Transaction
                                     .Inputs
                                     .Select(txin => Task.Run(() =>
                                     {
                                         var parentTx = GetTransactions(false, new uint256[] { txin.PrevOut.Hash }).FirstOrDefault();
                                         if (parentTx == null)
                                         {
                                             IndexerTrace.MissingTransactionFromDatabase(txin.PrevOut.Hash);
                                             return null;
                                         }
                                         return parentTx.Transaction.Outputs[(int)txin.PrevOut.N];
                                     }))
                                     .ToArray();

                            Task.WaitAll(tasks);
                            if (tasks.All(t => t.Result != null))
                            {
                                var outputs = tasks.Select(t => t.Result).ToArray();
                                result[i].SpentCoins = outputs.Select((o, n) => new Spendable(result[i].Transaction.Inputs[n].PrevOut, o)).ToList();
                                entities[0].PreviousTxOuts.Clear();
                                entities[0].PreviousTxOuts.AddRange(outputs);
                                if (entities[0].IsLoaded)
                                {
                                    table.Execute(TableOperation.Merge(entities[0].CreateTableEntity()));
                                }
                            }
                        }

                        if (result[i].Transaction == null)
                            result[i] = null;
                    }
                });
            }
            catch (AggregateException ex)
            {
                throw ex.InnerException;
            }
            return result;
        }

        public ChainBlockHeader GetBestBlock()
        {
            var table = Configuration.GetChainTable();
            var part = table.ExecuteQuery(new TableQuery()
            {
                TakeCount = 1
            }).Select(e => new ChainPartEntry(e)).FirstOrDefault();
            if (part == null)
                return null;

            var block = part.BlockHeaders[part.BlockHeaders.Count - 1];
            return new ChainBlockHeader()
            {
                BlockId = block.GetHash(),
                Header = block,
                Height = part.ChainOffset + part.BlockHeaders.Count - 1
            };
        }

        public IEnumerable<ChainBlockHeader> GetChainChangesUntilFork(ChainedBlock currentTip, bool forkIncluded, CancellationToken cancellation = default(CancellationToken))
        {
            var table = Configuration.GetChainTable();
            List<ChainBlockHeader> blocks = new List<ChainBlockHeader>();
            foreach (var chainPart in
            table.ExecuteQuery(new TableQuery()
            {
                TakeCount = 2   //If almost synchronized, then it won't affect too much table throttling
            })
            .Concat(table.ExecuteQuery(new TableQuery()).Skip(2))
            .Select(e => new ChainPartEntry(e)))
            {
                cancellation.ThrowIfCancellationRequested();

                int height = chainPart.ChainOffset + chainPart.BlockHeaders.Count - 1;
                foreach (var block in chainPart.BlockHeaders.Reverse<BlockHeader>())
                {
                    if (height > currentTip.Height)
                        yield return CreateChainChange(height, block);
                    else
                    {
                        if (height < currentTip.Height)
                            currentTip = currentTip.FindAncestorOrSelf(height);
                        var chainChange = CreateChainChange(height, block);
                        if (chainChange.BlockId == currentTip.HashBlock)
                        {
                            if (forkIncluded)
                                yield return chainChange;
                            yield break;
                        }
                        yield return chainChange;
                        currentTip = currentTip.Previous;
                    }
                    height--;
                }
            }
        }

        private ChainBlockHeader CreateChainChange(int height, BlockHeader block)
        {
            return new ChainBlockHeader()
                       {
                           Height = height,
                           Header = block,
                           BlockId = block.GetHash()
                       };
        }

        Dictionary<string, Func<WalletRule>> _Rules = new Dictionary<string, Func<WalletRule>>();
        public WalletRuleEntry[] GetWalletRules(string walletId)
        {
            var table = Configuration.GetWalletRulesTable();
            var searchedEntity = new WalletRuleEntry(walletId, null).CreateTableEntity(Configuration.SerializerSettings);
            var query = new TableQuery()
                                    .Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, searchedEntity.PartitionKey));
            return
                table.ExecuteQuery(query)
                 .Select(e => new WalletRuleEntry(e, this))
                 .ToArray();
        }



        public WalletRuleEntry AddWalletRule(string walletId, WalletRule walletRule)
        {
            var table = Configuration.GetWalletRulesTable();
            var entry = new WalletRuleEntry(walletId, walletRule);
            var entity = entry.CreateTableEntity(Configuration.SerializerSettings);
            table.Execute(TableOperation.InsertOrReplace(entity));
            return entry;
        }



        public WalletRuleEntryCollection GetAllWalletRules()
        {
            return
                new WalletRuleEntryCollection(
                Configuration.GetWalletRulesTable()
                .ExecuteQuery(new TableQuery())
                .Select(e => new WalletRuleEntry(e, this)));
        }

        public bool ColoredBalance
        {
            get;
            set;
        }

        internal WalletRule Deserialize(string rule)
        {
            return (WalletRule)JsonConvert.DeserializeObject<ICustomData>(rule, Configuration.SerializerSettings);
        }


        public IEnumerable<OrderedBalanceChange> GetOrderedBalance(string walletId, CancellationToken cancel = default(CancellationToken))
        {
            return GetOrderedBalanceCore(OrderedBalanceChange.GetBalanceId(walletId), cancel);
        }
        public IEnumerable<OrderedBalanceChange> GetOrderedBalance(IDestination destination, CancellationToken cancel = default(CancellationToken))
        {
            return GetOrderedBalance(destination.ScriptPubKey, cancel);
        }


        public IEnumerable<OrderedBalanceChange> GetOrderedBalance(Script scriptPubKey, CancellationToken cancel = default(CancellationToken))
        {
            return GetOrderedBalanceCore(OrderedBalanceChange.GetBalanceId(scriptPubKey), cancel);
        }

        private IEnumerable<OrderedBalanceChange> GetOrderedBalanceCore(string balanceId, CancellationToken cancel)
        {
            Queue<OrderedBalanceChange> unconfirmed = new Queue<OrderedBalanceChange>();
            List<OrderedBalanceChange> unconformedList = new List<OrderedBalanceChange>();
            var table = Configuration.GetBalanceTable();
            foreach (var c in QueryBalance(balanceId, table))
            {
                cancel.ThrowIfCancellationRequested();
                var change = new OrderedBalanceChange(c, Configuration.SerializerSettings);
                if (change.BlockId == null)
                    unconformedList.Add(change);
                else
                {
                    if (unconformedList != null)
                    {
                        unconfirmed = new Queue<OrderedBalanceChange>(unconformedList.OrderByDescending(o => o.SeenUtc));
                        unconformedList = null;
                    }

                    while (unconfirmed.Count != 0 && change.SeenUtc < unconfirmed.Peek().SeenUtc)
                    {
                        var unconfirmedChange = unconfirmed.Dequeue();
                        EnsurePreviousLoaded(unconfirmedChange);
                        yield return unconfirmedChange;
                    }

                    EnsurePreviousLoaded(change);
                    yield return change;
                }
            }
            if (unconformedList != null)
            {
                unconfirmed = new Queue<OrderedBalanceChange>(unconformedList.OrderByDescending(o => o.SeenUtc));
                unconformedList = null;
            }
            while (unconfirmed.Count != 0)
            {
                var change = unconfirmed.Dequeue();
                EnsurePreviousLoaded(change);
                yield return change;
            }
        }

        private static IEnumerable<DynamicTableEntity> QueryBalance(string balanceId, CloudTable table)
        {
            var partition = OrderedBalanceChange.GetPartitionKey(balanceId);
            return table.ExecuteQuery(CreateQuery(partition, balanceId));
        }

        private static TableQuery CreateQuery(string partition, string startWith)
        {
            return new TableQuery()
            {
                FilterString =
                TableQuery.CombineFilters(
                                            TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, partition),
                                            TableOperators.And,
                                            TableQuery.CombineFilters(
                                                TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.GreaterThan, startWith + "-"),
                                                TableOperators.And,
                                                TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.LessThan, startWith + "|")
                                            ))
            };
        }

        public void CleanUnconfirmedChanges(IDestination destination, TimeSpan olderThan)
        {
            CleanUnconfirmedChanges(destination.ScriptPubKey, olderThan);
        }

        public void CleanUnconfirmedChanges(Script scriptPubKey, TimeSpan olderThan)
        {
            var table = Configuration.GetBalanceTable();
            List<DynamicTableEntity> unconfirmed = new List<DynamicTableEntity>();
            foreach (var c in QueryBalance(OrderedBalanceChange.GetBalanceId(scriptPubKey), table))
            {
                var change = new OrderedBalanceChange(c, Configuration.SerializerSettings);
                if (change.BlockId != null)
                    break;
                if (DateTime.UtcNow - change.SeenUtc < olderThan)
                    continue;
                unconfirmed.Add(c);
            }

            Parallel.ForEach(unconfirmed, c =>
            {
                var t = Configuration.GetBalanceTable();
                c.ETag = "*";
                t.Execute(TableOperation.Delete(c));
            });
        }

        public bool EnsurePreviousLoaded(OrderedBalanceChange change)
        {
            if (change.SpentCoins != null)
            {
                if (change.ColoredBalanceChangeEntry != null || !ColoredBalance)
                    return true;
            }



            var transactions =
                GetTransactions(false, ColoredBalance, change.SpentOutpoints.Select(s => s.Hash).ToArray());
            CoinCollection result = new CoinCollection();
            for (int i = 0 ; i < transactions.Length ; i++)
            {
                var outpoint = change.SpentOutpoints[i];
                if (outpoint.IsNull)
                    continue;
                var prev = transactions[i];
                if (prev == null)
                    return false;
                if (ColoredBalance && prev.ColoredTransaction == null)
                    return false;
                result.Add(new Coin(outpoint, prev.Transaction.Outputs[change.SpentOutpoints[i].N]));
            }
            change.SpentCoins = result;


            if (ColoredBalance && change.ColoredBalanceChangeEntry == null)
            {
                var thisTransaction = GetTransactions(false, ColoredBalance, new[] { change.TransactionId })[0];
                if (thisTransaction.ColoredTransaction == null)
                    return false;
                change.ColoredBalanceChangeEntry = new ColoredBalanceChangeEntry(change, thisTransaction.ColoredTransaction);
            }

            var entity = change.ToEntity(Configuration.SerializerSettings);
            var spentCoins = Helper.GetEntityProperty(entity, "b");
            var coloredTx = ColoredBalance ? entity.Properties["g"].BinaryValue : null;
            entity.Properties.Clear();
            if (coloredTx != null)
                entity.Properties.Add("g", new EntityProperty(coloredTx));
            Helper.SetEntityProperty(entity, "b", spentCoins);
            Configuration.GetBalanceTable().Execute(TableOperation.Merge(entity));
            return true;
        }

        public void PruneBalances(IEnumerable<OrderedBalanceChange> balances)
        {
            Parallel.ForEach(balances, b =>
            {
                var table = Configuration.GetBalanceTable();
                table.Execute(TableOperation.Delete(b.ToEntity(Configuration.SerializerSettings)));
            });
        }

        public ConcurrentChain GetMainChain()
        {
            ConcurrentChain chain = new ConcurrentChain();
            SynchronizeChain(chain);
            return chain;
        }

        public void SynchronizeChain(ChainBase chain)
        {
            if (chain.Tip != null && chain.Genesis.HashBlock != Configuration.Network.GetGenesis().GetHash())
                throw new ArgumentException("Incompatible Network between the indexer and the chain", "chain");
            if (chain.Tip == null)
                chain.SetTip(new ChainedBlock(Configuration.Network.GetGenesis().Header, 0));
            GetChainChangesUntilFork(chain.Tip, false)
                .UpdateChain(chain);
        }

        public void MergeIntoWallet(string walletId, IDestination destination, CancellationToken cancel = default(CancellationToken))
        {
            MergeIntoWallet(walletId, destination.ScriptPubKey, cancel);
        }

        public void MergeIntoWallet(string walletId, Script scriptPubKey, CancellationToken cancel = default(CancellationToken))
        {
            MergeIntoWalletCore(walletId, OrderedBalanceChange.GetBalanceId(scriptPubKey), cancel);
        }

        public void MergeIntoWallet(string walletId, string walletSource, CancellationToken cancel = default(CancellationToken))
        {
            MergeIntoWalletCore(walletId, OrderedBalanceChange.GetBalanceId(walletSource), cancel);
        }

        private void MergeIntoWalletCore(string walletId, string balanceId, CancellationToken cancel)
        {
            var indexer = Configuration.CreateIndexer();
            var sourcesByKey = GetOrderedBalanceCore(balanceId, cancel)
                .ToDictionary(i => GetKey(i));
            var destByKey =
                GetOrderedBalance(walletId, cancel)
                .ToDictionary(i => GetKey(i));

            List<OrderedBalanceChange> entities = new List<OrderedBalanceChange>();
            foreach (var kv in sourcesByKey)
            {
                var source = kv.Value;
                var existing = destByKey.TryGet(kv.Key);
                if (existing == null)
                {
                    existing = new OrderedBalanceChange(walletId, source);
                }
                existing.Merge(kv.Value, null);
                entities.Add(existing);
                if (entities.Count == 100)
                    indexer.Index(entities);
            }
            if (entities.Count != 0)
                indexer.Index(entities);
        }

        private string GetKey(OrderedBalanceChange change)
        {
            return change.Height + "-" + (change.BlockId == null ? new uint256(0) : change.BlockId) + "-" + change.TransactionId;
        }
    }
}
