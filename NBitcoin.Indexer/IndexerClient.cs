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
            BalancePartitionSize = 50;
        }

        public int BalancePartitionSize
        {
            get;
            set;
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
            return GetTransactionAsync(lazyLoadSpentOutput, txId).Result;
        }
        public Task<TransactionEntry> GetTransactionAsync(bool lazyLoadSpentOutput, uint256 txId)
        {
            return GetTransactionAsync(lazyLoadSpentOutput, false, txId);
        }
        public TransactionEntry GetTransaction(uint256 txId)
        {
            return GetTransactionAsync(txId).Result;
        }
        public Task<TransactionEntry> GetTransactionAsync(uint256 txId)
        {
            return GetTransactionAsync(true, false, txId);
        }

        public TransactionEntry[] GetTransactions(bool lazyLoadPreviousOutput, uint256[] txIds)
        {
            return GetTransactionsAsync(lazyLoadPreviousOutput, txIds).Result;
        }
        public Task<TransactionEntry[]> GetTransactionsAsync(bool lazyLoadPreviousOutput, uint256[] txIds)
        {
            return GetTransactionsAsync(lazyLoadPreviousOutput, false, txIds);
        }

        public async Task<TransactionEntry> GetTransactionAsync(bool lazyLoadPreviousOutput, bool fetchColor, uint256 txId)
        {
            if (txId == null)
                return null;
            TransactionEntry result = null;

            var table = Configuration.GetTransactionTable();
            var searchedEntity = new TransactionEntry.Entity(txId);
            var query = new TableQuery()
                            .Where(
                                    TableQuery.CombineFilters(
                                        TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, searchedEntity.PartitionKey),
                                        TableOperators.And,
                                        TableQuery.CombineFilters(
                                            TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.GreaterThan, txId.ToString() + "-"),
                                            TableOperators.And,
                                            TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.LessThan, txId.ToString() + "|")
                                        )
                                  ));
            query.TakeCount = 10; //Should not have more
            var entities = (await table.ExecuteQuerySegmentedAsync(query, null).ConfigureAwait(false))
                               .Select(e => new TransactionEntry.Entity(e)).ToArray();
            if (entities.Length == 0)
                result = null;
            else
            {
                result = new TransactionEntry(entities);
                if (result.Transaction == null)
                {
                    foreach (var block in result.BlockIds.Select(id => GetBlock(id)).Where(b => b != null))
                    {
                        result.Transaction = block.Transactions.FirstOrDefault(t => t.GetHash() == txId);
                        entities[0].Transaction = result.Transaction;
                        if (entities[0].Transaction != null)
                        {
                            await table.ExecuteAsync(TableOperation.Merge(entities[0].CreateTableEntity())).ConfigureAwait(false);
                        }
                        break;
                    }
                }

                if (fetchColor && result.ColoredTransaction == null)
                {
                    result.ColoredTransaction = ColoredTransaction.FetchColors(txId, result.Transaction, new IndexerColoredTransactionRepository(Configuration));
                    entities[0].ColoredTransaction = result.ColoredTransaction;
                    if (entities[0].ColoredTransaction != null)
                    {
                        await table.ExecuteAsync(TableOperation.Merge(entities[0].CreateTableEntity())).ConfigureAwait(false);
                    }
                }

                var needTxOut = result.SpentCoins == null && lazyLoadPreviousOutput && result.Transaction != null;
                if (needTxOut)
                {
                    var tasks =
                        result.Transaction
                             .Inputs
                             .Select(async txin =>
                             {
                                 var parentTx = await GetTransactionAsync(false, false, txin.PrevOut.Hash).ConfigureAwait(false);
                                 if (parentTx == null)
                                 {
                                     IndexerTrace.MissingTransactionFromDatabase(txin.PrevOut.Hash);
                                     return null;
                                 }
                                 return parentTx.Transaction.Outputs[(int)txin.PrevOut.N];
                             })
                             .ToArray();

                    await Task.WhenAll(tasks).ConfigureAwait(false);
                    if (tasks.All(t => t.Result != null))
                    {
                        var outputs = tasks.Select(t => t.Result).ToArray();
                        result.SpentCoins = outputs.Select((o, n) => new Spendable(result.Transaction.Inputs[n].PrevOut, o)).ToList();
                        entities[0].PreviousTxOuts.Clear();
                        entities[0].PreviousTxOuts.AddRange(outputs);
                        if (entities[0].IsLoaded)
                        {
                            await table.ExecuteAsync(TableOperation.Merge(entities[0].CreateTableEntity())).ConfigureAwait(false);
                        }
                    }
                }

                if (result.Transaction == null)
                    result = null;
            }

            return result;
        }

        /// <summary>
        /// Get transactions in Azure Table
        /// </summary>
        /// <param name="txIds"></param>
        /// <returns>All transactions (with null entries for unfound transactions)</returns>
        public async Task<TransactionEntry[]> GetTransactionsAsync(bool lazyLoadPreviousOutput, bool fetchColor, uint256[] txIds)
        {
            var result = new TransactionEntry[txIds.Length];
            var queries = new TableQuery[txIds.Length];
            var tasks = Enumerable.Range(0, txIds.Length)
                .Select(async (i) =>
                {
                    result[i] = await GetTransactionAsync(lazyLoadPreviousOutput, fetchColor, txIds[i]).ConfigureAwait(false);
                }).ToArray();

            await Task.WhenAll(tasks).ConfigureAwait(false);
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


        public IEnumerable<OrderedBalanceChange> GetOrderedBalance(string walletId,
                                                                   BalanceQuery query = null,
                                                                   CancellationToken cancel = default(CancellationToken))
        {
            return GetOrderedBalanceCore(OrderedBalanceChange.GetBalanceId(walletId), query, cancel);
        }
        public IEnumerable<Task<List<OrderedBalanceChange>>> GetOrderedBalanceAsync(string walletId,
                                                                  BalanceQuery query = null,
                                                                  CancellationToken cancel = default(CancellationToken))
        {
            return GetOrderedBalanceCoreAsync(OrderedBalanceChange.GetBalanceId(walletId), query, cancel);
        }
        public IEnumerable<OrderedBalanceChange> GetOrderedBalance(IDestination destination, BalanceQuery query = null, CancellationToken cancel = default(CancellationToken))
        {
            return GetOrderedBalance(destination.ScriptPubKey, query, cancel);
        }
        public IEnumerable<Task<List<OrderedBalanceChange>>> GetOrderedBalanceAsync(IDestination destination, BalanceQuery query = null, CancellationToken cancel = default(CancellationToken))
        {
            return GetOrderedBalanceAsync(destination.ScriptPubKey, query, cancel);
        }


        public IEnumerable<OrderedBalanceChange> GetOrderedBalance(Script scriptPubKey, BalanceQuery query = null, CancellationToken cancel = default(CancellationToken))
        {
            return GetOrderedBalanceCore(OrderedBalanceChange.GetBalanceId(scriptPubKey), query, cancel);
        }
        public IEnumerable<Task<List<OrderedBalanceChange>>> GetOrderedBalanceAsync(Script scriptPubKey, BalanceQuery query = null, CancellationToken cancel = default(CancellationToken))
        {
            return GetOrderedBalanceCoreAsync(OrderedBalanceChange.GetBalanceId(scriptPubKey), query, cancel);
        }

        private IEnumerable<OrderedBalanceChange> GetOrderedBalanceCore(string balanceId, BalanceQuery query, CancellationToken cancel)
        {
            foreach (var partition in GetOrderedBalanceCoreAsync(balanceId, query, cancel))
            {
                foreach (var change in partition.Result)
                {
                    yield return change;
                }
            }
        }

        private IEnumerable<Task<List<OrderedBalanceChange>>> GetOrderedBalanceCoreAsync(string balanceId, BalanceQuery query, CancellationToken cancel)
        {
            if (query == null)
                query = new BalanceQuery();
            Queue<OrderedBalanceChange> unconfirmed = new Queue<OrderedBalanceChange>();
            List<OrderedBalanceChange> unconfirmedList = new List<OrderedBalanceChange>();

            List<OrderedBalanceChange> result = new List<OrderedBalanceChange>();

            var table = Configuration.GetBalanceTable();

            var entityQuery = query.CreateEntityQuery(balanceId);


            var partitions =
                table.ExecuteQuery(entityQuery)
                 .Select(c => new OrderedBalanceChange(c, Configuration.SerializerSettings))
                 .Select(c => new
                      {
                          Loaded = NeedLoading(c) ? EnsurePreviousLoadedAsync(c) : Task.FromResult(true),
                          Change = c
                      })
                 .Partition(BalancePartitionSize);

            foreach (var partition in partitions)
            {
                cancel.ThrowIfCancellationRequested();
                var partitionLoading = Task.WhenAll(partition.Select(_ => _.Loaded));
                foreach (var change in partition.Select(p => p.Change))
                {
                    if (change.BlockId == null)
                        unconfirmedList.Add(change);
                    else
                    {
                        if (unconfirmedList != null)
                        {
                            unconfirmed = new Queue<OrderedBalanceChange>(unconfirmedList.OrderByDescending(o => o.SeenUtc));
                            unconfirmedList = null;
                        }

                        while (unconfirmed.Count != 0 && change.SeenUtc < unconfirmed.Peek().SeenUtc)
                        {
                            var unconfirmedChange = unconfirmed.Dequeue();
                            result.Add(unconfirmedChange);
                        }
                        result.Add(change);
                    }
                }
                yield return WaitAndReturn(partitionLoading, result);
                result = new List<OrderedBalanceChange>();
            }
            if (unconfirmedList != null)
            {
                unconfirmed = new Queue<OrderedBalanceChange>(unconfirmedList.OrderByDescending(o => o.SeenUtc));
                unconfirmedList = null;
            }
            while (unconfirmed.Count != 0)
            {
                var change = unconfirmed.Dequeue();
                result.Add(change);
            }
            if (result.Count > 0)
                yield return WaitAndReturn(null, result);
        }

        private async Task<List<OrderedBalanceChange>> WaitAndReturn(Task<bool[]> partitionLoading, List<OrderedBalanceChange> result)
        {
            if (partitionLoading != null)
                await Task.WhenAll(partitionLoading);
            return result;
        }

        public void CleanUnconfirmedChanges(IDestination destination, TimeSpan olderThan)
        {
            CleanUnconfirmedChanges(destination.ScriptPubKey, olderThan);
        }

        public void CleanUnconfirmedChanges(Script scriptPubKey, TimeSpan olderThan)
        {
            var table = Configuration.GetBalanceTable();
            List<DynamicTableEntity> unconfirmed = new List<DynamicTableEntity>();
            foreach (var c in table.ExecuteQuery(new BalanceQuery().CreateEntityQuery(OrderedBalanceChange.GetBalanceId(scriptPubKey))))
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

        private bool NeedLoading(OrderedBalanceChange change)
        {
            if (change.SpentCoins != null)
            {
                if (change.ColoredBalanceChangeEntry != null || !ColoredBalance)
                {
                    change.AddRedeemInfo();
                    return false;
                }
            }
            return true;
        }

        public async Task<bool> EnsurePreviousLoadedAsync(OrderedBalanceChange change)
        {
            if (!NeedLoading(change))
                return true;
            var transactions =
                await GetTransactionsAsync(false, ColoredBalance, change.SpentOutpoints.Select(s => s.Hash).ToArray()).ConfigureAwait(false);
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
                var thisTransaction = await GetTransactionAsync(false, ColoredBalance, change.TransactionId).ConfigureAwait(false);
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
            change.AddRedeemInfo();
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
            var sourcesByKey = GetOrderedBalanceCore(balanceId, null, cancel)
                .ToDictionary(i => GetKey(i));
            var destByKey =
                GetOrderedBalance(walletId, null, cancel)
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
