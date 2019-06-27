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
using System.Collections.Async;

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
        public ConsensusFactory ConsensusFactory => Configuration.Network.Consensus.ConsensusFactory;
        public IndexerClient(IndexerConfiguration configuration)
        {
            if(configuration == null)
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
                container.GetPageBlobReference(blockId.ToString()).DownloadToStreamAsync(ms).GetAwaiter().GetResult();
                ms.Position = 0;
                Block b = ConsensusFactory.CreateBlock();
                b.ReadWrite(ms, false, Configuration.Network);
                return b;
            }
            catch(StorageException ex)
            {
                if(ex.RequestInformation != null && ex.RequestInformation.HttpStatusCode == 404)
                {
                    return null;
                }
                throw;
            }
        }

        public TransactionEntry GetTransaction(bool loadPreviousOutput, uint256 txId)
        {
            return GetTransactionAsync(loadPreviousOutput, txId).Result;
        }
        public Task<TransactionEntry> GetTransactionAsync(bool loadPreviousOutput, uint256 txId)
        {
            return GetTransactionAsync(loadPreviousOutput, false, txId);
        }
        public TransactionEntry GetTransaction(uint256 txId)
        {
            return GetTransactionAsync(txId).Result;
        }
        public Task<TransactionEntry> GetTransactionAsync(uint256 txId)
        {
            return GetTransactionAsync(true, false, txId);
        }

        public TransactionEntry[] GetTransactions(bool loadPreviousOutput, uint256[] txIds)
        {
            return GetTransactionsAsync(loadPreviousOutput, txIds).Result;
        }
        public Task<TransactionEntry[]> GetTransactionsAsync(bool loadPreviousOutput, uint256[] txIds)
        {
            return GetTransactionsAsync(loadPreviousOutput, false, txIds);
        }

        public async Task<TransactionEntry> GetTransactionAsync(bool loadPreviousOutput, bool fetchColor, uint256 txId)
        {
            if(txId == null)
                return null;
            TransactionEntry result = null;

            var table = Configuration.GetTransactionTable();
            var searchedEntity = new TransactionEntry.Entity(txId);
            var query = new TableQuery<DynamicTableEntity>()
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
            List<TransactionEntry.Entity> entities = new List<TransactionEntry.Entity>();
            foreach(var e in await table.ExecuteQuerySegmentedAsync(query, null).ConfigureAwait(false))
            {
                if(e.IsFat())
                    entities.Add(new TransactionEntry.Entity(await FetchFatEntity(e).ConfigureAwait(false), ConsensuFactory));
                else
                    entities.Add(new TransactionEntry.Entity(e, ConsensuFactory));
            }
            if(entities.Count == 0)
                result = null;
            else
            {
                result = new TransactionEntry(entities.ToArray());
                if(result.Transaction == null)
                {
                    foreach(var block in result.BlockIds.Select(id => GetBlock(id)).Where(b => b != null))
                    {
                        result.Transaction = block.Transactions.FirstOrDefault(t => t.GetHash() == txId);
                        entities[0].Transaction = result.Transaction;
                        if(entities[0].Transaction != null)
                        {
                            await UpdateEntity(table, entities[0].CreateTableEntity()).ConfigureAwait(false);
                        }
                        break;
                    }
                }

                if(fetchColor && result.ColoredTransaction == null)
                {
                    result.ColoredTransaction = await ColoredTransaction.FetchColorsAsync(txId, result.Transaction, new CachedColoredTransactionRepository(new IndexerColoredTransactionRepository(Configuration))).ConfigureAwait(false);
                    entities[0].ColoredTransaction = result.ColoredTransaction;
                    if(entities[0].ColoredTransaction != null)
                    {
                        await UpdateEntity(table, entities[0].CreateTableEntity()).ConfigureAwait(false);
                    }
                }
                var needTxOut = result.SpentCoins == null && loadPreviousOutput && result.Transaction != null;
                if(needTxOut)
                {
                    var inputs = result.Transaction.Inputs.Select(o => o.PrevOut).ToArray();
                    var parents = await
                            GetTransactionsAsync(false, false, inputs
                             .Select(i => i.Hash)
                             .ToArray()).ConfigureAwait(false);

                    for(int i = 0; i < parents.Length; i++)
                    {
                        if(parents[i] == null)
                        {
                            IndexerTrace.MissingTransactionFromDatabase(result.Transaction.Inputs[i].PrevOut.Hash);
                            return null;
                        }
                    }

                    var outputs = parents.Select((p, i) => p.Transaction.Outputs[inputs[i].N]).ToArray();

                    result.SpentCoins = Enumerable
                                            .Range(0, inputs.Length)
                                            .Select(i => new Spendable(inputs[i], outputs[i]))
                                            .ToList();
                    entities[0].PreviousTxOuts.Clear();
                    entities[0].PreviousTxOuts.AddRange(outputs);
                    if(entities[0].IsLoaded)
                    {
                        await UpdateEntity(table, entities[0].CreateTableEntity()).ConfigureAwait(false);
                    }
                }
            }
            return result != null && result.Transaction != null ? result : null;
        }

        private async Task UpdateEntity(CloudTable table, DynamicTableEntity entity)
        {
            try
            {
                await table.ExecuteAsync(TableOperation.Merge(entity)).ConfigureAwait(false);
                return;
            }
            catch(StorageException ex)
            {
                if(!Helper.IsError(ex, "EntityTooLarge"))
                    throw;
            }
            var serialized = entity.Serialize();
            Configuration
                    .GetBlocksContainer()
                    .GetBlockBlobReference(entity.GetFatBlobName())
                    .UploadFromByteArrayAsync(serialized, 0, serialized.Length).GetAwaiter().GetResult();
            entity.MakeFat(serialized.Length);
            await table.ExecuteAsync(TableOperation.InsertOrReplace(entity)).ConfigureAwait(false);
        }

        private async Task<DynamicTableEntity> FetchFatEntity(DynamicTableEntity e)
        {
            var size = e.Properties["fat"].Int32Value.Value;
            byte[] bytes = new byte[size];
            await Configuration
                .GetBlocksContainer()
                .GetBlockBlobReference(e.GetFatBlobName())
                .DownloadRangeToByteArrayAsync(bytes, 0, 0, bytes.Length).ConfigureAwait(false);
            e = new DynamicTableEntity();
            e.Deserialize(bytes);
            return e;
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
                        .Select(i => new
                        {
                            TxId = txIds[i],
                            Index = i
                        })
                        .GroupBy(o => o.TxId, o => o.Index)
                        .Select(async (o) =>
                        {
                            var transaction = await GetTransactionAsync(lazyLoadPreviousOutput, fetchColor, o.Key).ConfigureAwait(false);
                            foreach(var index in o)
                            {
                                result[index] = transaction;
                            }
                        }).ToArray();

            await Task.WhenAll(tasks).ConfigureAwait(false);
            return result;
        }

        public ChainBlockHeader GetBestBlock()
        {
            var table = Configuration.GetChainTable();
            var part = table.ExecuteQueryAsync(new TableQuery<DynamicTableEntity>()
            {
                TakeCount = 1
            }).GetAwaiter().GetResult().Select(e => new ChainPartEntry(e, ConsensuFactory)).FirstOrDefault();
            if(part == null)
                return null;

            var block = part.BlockHeaders[part.BlockHeaders.Count - 1];
            return new ChainBlockHeader()
            {
                BlockId = block.GetHash(),
                Header = block,
                Height = part.ChainOffset + part.BlockHeaders.Count - 1
            };
        }

        ConsensusFactory ConsensuFactory => Configuration.Network.Consensus.ConsensusFactory;

        public IAsyncEnumerable<ChainBlockHeader> GetChainChangesUntilFork(ChainedBlock currentTip, bool forkIncluded, CancellationToken cancellation = default(CancellationToken))
        {
            return new AsyncEnumerable<ChainBlockHeader>(async yield =>
            {
                var oldTip = currentTip;
                var table = Configuration.GetChainTable();
                List<ChainBlockHeader> blocks = new List<ChainBlockHeader>();

                async Task ProcessChainPart(ChainPartEntry chainPart)
                {
                    int height = chainPart.ChainOffset + chainPart.BlockHeaders.Count - 1;
                    foreach (var block in chainPart.BlockHeaders.Reverse<BlockHeader>())
                    {
                        if (currentTip == null && oldTip != null)
                            throw new InvalidOperationException("No fork found, the chain stored in azure is probably different from the one of the provided input");
                        if (oldTip == null || height > currentTip.Height)
                            await yield.ReturnAsync(CreateChainChange(height, block));
                        else
                        {
                            if (height < currentTip.Height)
                                currentTip = currentTip.FindAncestorOrSelf(height);
                            var chainChange = CreateChainChange(height, block);
                            if (chainChange.BlockId == currentTip.HashBlock)
                            {
                                if (forkIncluded)
                                    await yield.ReturnAsync(chainChange);
                                yield.Break();
                            }
                            await yield.ReturnAsync(chainChange);
                            currentTip = currentTip.Previous;
                        }
                        height--;
                    }
                }


                var enumerator = await ExecuteBalanceQuery(table, new TableQuery<DynamicTableEntity>(), new[] { 1, 2, 10 }).GetAsyncEnumeratorAsync(cancellation);
                while (await enumerator.MoveNextAsync(cancellation))
                {
                    var chainPart = new ChainPartEntry(enumerator.Current, ConsensuFactory);
                    await ProcessChainPart(chainPart);
                }
                foreach (var chainPart in (await table.ExecuteQueryAsync(new TableQuery<DynamicTableEntity>())).Skip(2)
                            .Select(e => new ChainPartEntry(e, ConsensuFactory)))
                {
                    await ProcessChainPart(chainPart);
                }
            });
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
            var searchedEntity = new WalletRuleEntry(walletId, null).CreateTableEntity();
            var query = new TableQuery<DynamicTableEntity>()
                                    .Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, searchedEntity.PartitionKey));
            return
                table.ExecuteQueryAsync(query).GetAwaiter().GetResult()
                 .Select(e => new WalletRuleEntry(e, this))
                 .ToArray();
        }



        public WalletRuleEntry AddWalletRule(string walletId, WalletRule walletRule)
        {
            var table = Configuration.GetWalletRulesTable();
            var entry = new WalletRuleEntry(walletId, walletRule);
            var entity = entry.CreateTableEntity();
            table.ExecuteAsync(TableOperation.InsertOrReplace(entity)).GetAwaiter().GetResult();
            return entry;
        }



        public WalletRuleEntryCollection GetAllWalletRules()
        {
            return
                new WalletRuleEntryCollection(
                Configuration.GetWalletRulesTable()
                .ExecuteQueryAsync(new TableQuery<DynamicTableEntity>()).GetAwaiter().GetResult()
                .Select(e => new WalletRuleEntry(e, this)));
        }

        public bool ColoredBalance
        {
            get;
            set;
        }


        public Task<ICollection<OrderedBalanceChange>> GetOrderedBalance(string walletId,
                                                                   BalanceQuery query = null,
                                                                   CancellationToken cancel = default(CancellationToken))
        {
            return GetOrderedBalanceCoreAsync(new BalanceId(walletId), query, cancel);
        }

        public Task<ICollection<OrderedBalanceChange>> GetOrderedBalance(BalanceId balanceId,
                                                                  BalanceQuery query = null,
                                                                  CancellationToken cancel = default(CancellationToken))
        {
            return GetOrderedBalanceCoreAsync(balanceId, query, cancel);
        }

        public Task<ICollection<OrderedBalanceChange>> GetOrderedBalanceAsync(BalanceId balanceId,
                                                                  BalanceQuery query = null,
                                                                  CancellationToken cancel = default(CancellationToken))
        {
            return GetOrderedBalanceCoreAsync(balanceId, query, cancel);
        }

        public Task<ICollection<OrderedBalanceChange>> GetOrderedBalanceAsync(string walletId,
                                                                  BalanceQuery query = null,
                                                                  CancellationToken cancel = default(CancellationToken))
        {
            return GetOrderedBalanceCoreAsync(new BalanceId(walletId), query, cancel);
        }
        public Task<ICollection<OrderedBalanceChange>> GetOrderedBalance(IDestination destination, BalanceQuery query = null, CancellationToken cancel = default(CancellationToken))
        {
            return GetOrderedBalance(destination.ScriptPubKey, query, cancel);
        }
        public Task<ICollection<OrderedBalanceChange>> GetOrderedBalanceAsync(IDestination destination, BalanceQuery query = null, CancellationToken cancel = default(CancellationToken))
        {
            return GetOrderedBalanceAsync(destination.ScriptPubKey, query, cancel);
        }


        public Task<ICollection<OrderedBalanceChange>> GetOrderedBalance(Script scriptPubKey, BalanceQuery query = null, CancellationToken cancel = default(CancellationToken))
        {
            return GetOrderedBalanceCoreAsync(new BalanceId(scriptPubKey), query, cancel);
        }
        public Task<ICollection<OrderedBalanceChange>> GetOrderedBalanceAsync(Script scriptPubKey, BalanceQuery query = null, CancellationToken cancel = default(CancellationToken))
        {
            return GetOrderedBalanceCoreAsync(new BalanceId(scriptPubKey), query, cancel);
        }

        private async Task<ICollection<OrderedBalanceChange>> GetOrderedBalanceCoreAsync(BalanceId balanceId, BalanceQuery query, CancellationToken cancel)
        {
            if(query == null)
                query = new BalanceQuery();

            var result = new List<OrderedBalanceChange>();

            var table = Configuration.GetBalanceTable();
            var tableQuery = ExecuteBalanceQuery(table, query.CreateTableQuery(balanceId), query.PageSizes);


            var partitions =
                  tableQuery
                 .Select(c => new OrderedBalanceChange(c, ConsensusFactory))
                 .Partition(BalancePartitionSize, cancel);

            var enumerator = await partitions.GetAsyncEnumeratorAsync(cancel);
            while (await enumerator.MoveNextAsync(cancel))
            {
                var partition = enumerator.Current;
                await Task.WhenAll(partition.Select(c => NeedLoading(c) ? EnsurePreviousLoadedAsync(c) : Task.FromResult<bool>(true)));
                foreach (var entity in partition)
                {
                    if (Prepare(entity))
                        result.Add(entity);
                }
            }
            if (query.RawOrdering)
                return result;
            result = result.TopologicalSort();
            result.Reverse();
            return result;
        }

        private bool Prepare(OrderedBalanceChange change)
        {
            change.UpdateToScriptCoins();
            if(change.SpentCoins == null || change.ReceivedCoins == null)
                return false;
            if(change.IsEmpty)
                return false;
            if(ColoredBalance)
            {
                if(change.ColoredTransaction == null)
                    return false;
                change.UpdateToColoredCoins();
            }
            return true;
        }

        private IAsyncEnumerable<DynamicTableEntity> ExecuteBalanceQuery(CloudTable table, TableQuery<DynamicTableEntity> tableQuery, IEnumerable<int> pages)
        {
            return new AsyncEnumerable<DynamicTableEntity>(async yield =>
            {
                pages = pages ?? new int[0];
                var pagesEnumerator = pages.GetEnumerator();
                TableContinuationToken continuation = null;
                do
                {
                    tableQuery.TakeCount = pagesEnumerator.MoveNext() ? (int?)pagesEnumerator.Current : null;

                    var segment = await table.ExecuteQuerySegmentedAsync<DynamicTableEntity>(tableQuery, continuation);
                    continuation = segment.ContinuationToken;
                    foreach (var entity in segment)
                    {
                        await yield.ReturnAsync(entity);
                    }
                } while (continuation != null);
            });
        }

        public void CleanUnconfirmedChanges(IDestination destination, TimeSpan olderThan)
        {
            CleanUnconfirmedChanges(destination.ScriptPubKey, olderThan);
        }



        public void CleanUnconfirmedChanges(Script scriptPubKey, TimeSpan olderThan)
        {
            var table = Configuration.GetBalanceTable();
            List<DynamicTableEntity> unconfirmed = new List<DynamicTableEntity>();

            foreach(var c in table.ExecuteQueryAsync(new BalanceQuery().CreateTableQuery(new BalanceId(scriptPubKey))).GetAwaiter().GetResult())
            {
                var change = new OrderedBalanceChange(c, ConsensusFactory);
                if(change.BlockId != null)
                    break;
                if(DateTime.UtcNow - change.SeenUtc < olderThan)
                    continue;
                unconfirmed.Add(c);
            }

            Parallel.ForEach(unconfirmed, c =>
            {
                var t = Configuration.GetBalanceTable();
                c.ETag = "*";
                t.ExecuteAsync(TableOperation.Delete(c)).GetAwaiter().GetResult();
            });
        }

        public bool NeedLoading(OrderedBalanceChange change)
        {
            if(change.SpentCoins != null)
            {
                if(change.ColoredTransaction != null || !ColoredBalance)
                {
                    return false;
                }
            }
            return true;
        }

        public async Task<bool> EnsurePreviousLoadedAsync(OrderedBalanceChange change)
        {
            if(!NeedLoading(change))
                return true;
            var parentIds = change.SpentOutpoints.Select(s => s.Hash).ToArray();
            var parents =
                await GetTransactionsAsync(false, ColoredBalance, parentIds).ConfigureAwait(false);

            var cache = new NoSqlTransactionRepository();
            foreach(var parent in parents.Where(p => p != null))
                cache.Put(parent.TransactionId, parent.Transaction);

            if(change.SpentCoins == null)
            {
                var success = await change.EnsureSpentCoinsLoadedAsync(cache).ConfigureAwait(false);
                if(!success)
                    return false;
            }
            if(ColoredBalance && change.ColoredTransaction == null)
            {
                var indexerRepo = new IndexerColoredTransactionRepository(Configuration);
                indexerRepo.Transactions = new CompositeTransactionRepository(new[] { new ReadOnlyTransactionRepository(cache), indexerRepo.Transactions });
                var success = await change.EnsureColoredTransactionLoadedAsync(indexerRepo).ConfigureAwait(false);
                if(!success)
                    return false;
            }
            var entity = change.ToEntity(ConsensusFactory);
            if(!change.IsEmpty)
            {
                await Configuration.GetBalanceTable().ExecuteAsync(TableOperation.Merge(entity)).ConfigureAwait(false);
            }
            else
            {
                try
                {
                    await Configuration.GetTransactionTable().ExecuteAsync(TableOperation.Delete(entity)).ConfigureAwait(false);
                }
                catch(StorageException ex)
                {
                    if(ex.RequestInformation == null || ex.RequestInformation.HttpStatusCode != 404)
                        throw;
                }
            }
            return true;
        }

        public void PruneBalances(IEnumerable<OrderedBalanceChange> balances)
        {
            Parallel.ForEach(balances, b =>
            {
                var table = Configuration.GetBalanceTable();
                table.ExecuteAsync(TableOperation.Delete(b.ToEntity(ConsensusFactory))).GetAwaiter().GetResult();
            });
        }

        public async Task<ConcurrentChain> GetMainChain(CancellationToken cancellationToken)
        {
            ConcurrentChain chain = new ConcurrentChain();
            await SynchronizeChain(chain, cancellationToken);
            return chain;
        }

        public async Task SynchronizeChain(ChainBase chain, CancellationToken cancellationToken)
        {
            if(chain.Tip != null && chain.Genesis.HashBlock != Configuration.Network.GetGenesis().GetHash())
                throw new ArgumentException("Incompatible Network between the indexer and the chain", "chain");
            if(chain.Tip == null)
                chain.SetTip(new ChainedBlock(Configuration.Network.GetGenesis().Header, 0));
            await GetChainChangesUntilFork(chain.Tip, false, cancellationToken)
                .UpdateChain(chain, cancellationToken);
        }

        public Task<bool> MergeIntoWallet(string walletId,
                                    IDestination destination,
                                    WalletRule rule = null,
                                    CancellationToken cancel = default(CancellationToken))
        {
            return MergeIntoWallet(walletId, destination.ScriptPubKey, rule, cancel);
        }

        public Task<bool> MergeIntoWallet(string walletId, Script scriptPubKey, WalletRule rule = null, CancellationToken cancel = default(CancellationToken))
        {
            return MergeIntoWalletCore(walletId, new BalanceId(scriptPubKey), rule, cancel);
        }

        public Task<bool> MergeIntoWallet(string walletId, string walletSource,
            WalletRule rule = null,
            CancellationToken cancel = default(CancellationToken))
        {
            return MergeIntoWalletCore(walletId, new BalanceId(walletSource), rule, cancel);
        }

        private async Task<bool> MergeIntoWalletCore(string walletId, BalanceId balanceId, WalletRule rule, CancellationToken cancel)
        {
            var indexer = Configuration.CreateIndexer();

            var query = new BalanceQuery()
            {
                From = new UnconfirmedBalanceLocator().Floor(),
                RawOrdering = true
            };
            var sourcesByKey = (await GetOrderedBalance(balanceId, query, cancel))
                .ToDictionary(i => GetKey(i));
            if(sourcesByKey.Count == 0)
                return false;
            var destByKey =
                (await GetOrderedBalance(walletId, query, cancel))
                .ToDictionary(i => GetKey(i));

            List<OrderedBalanceChange> entities = new List<OrderedBalanceChange>();
            foreach(var kv in sourcesByKey)
            {
                var source = kv.Value;
                var existing = destByKey.TryGet(kv.Key);
                if(existing == null)
                {
                    existing = new OrderedBalanceChange(walletId, source);
                }
                existing.Merge(kv.Value, rule);
                entities.Add(existing);
                if(entities.Count == 100)
                    indexer.Index(entities);
            }
            if(entities.Count != 0)
                indexer.Index(entities);
            return true;
        }

        private string GetKey(OrderedBalanceChange change)
        {
            return change.Height + "-" + (change.BlockId == null ? new uint256(0) : change.BlockId) + "-" + change.TransactionId + "-" + change.SeenUtc.Ticks;
        }
    }

    public static class CloudTableExtensions
    {
        // From: https://stackoverflow.com/questions/24234350/how-to-execute-an-azure-table-storage-query-async-client-version-4-0-1
        public static async Task<IList<DynamicTableEntity>> ExecuteQueryAsync(this CloudTable table, TableQuery<DynamicTableEntity> query, CancellationToken ct = default(CancellationToken), Action<IList<DynamicTableEntity>> onProgress = null)
        {

            var items = new List<DynamicTableEntity>();
            TableContinuationToken token = null;

            do
            {
                var seg = await table.ExecuteQuerySegmentedAsync(query, token);
                token = seg.ContinuationToken;
                items.AddRange(seg);
                if(onProgress != null) onProgress(items);

            } while(token != null && !ct.IsCancellationRequested);

            return items;
        }
    }
}
