using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Table;
using NBitcoin.Crypto;
using NBitcoin.DataEncoders;
using NBitcoin.Indexer.Converters;
using NBitcoin.Indexer.Internal;
using NBitcoin.Protocol;
using Newtonsoft.Json;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

namespace NBitcoin.Indexer
{
    public class IndexerServerConfiguration : IndexerConfiguration
    {
        public new static IndexerServerConfiguration FromConfiguration()
        {
            IndexerServerConfiguration config = new IndexerServerConfiguration();
            Fill(config);
            config.BlockDirectory = GetValue("BlockDirectory", true);
            config.MainDirectory = GetValue("MainDirectory", false);
            config.Node = GetValue("Node", false);
            return config;
        }
        public IndexerServerConfiguration()
        {
        }
        public string BlockDirectory
        {
            get;
            set;
        }
        public string MainDirectory
        {
            get;
            set;
        }
        public string GetFilePath(string name)
        {
            var fileName = StorageNamespace + name;
            if (!String.IsNullOrEmpty(MainDirectory))
                return Path.Combine(MainDirectory, fileName);
            return fileName;
        }
        public BlockStore CreateBlockStore()
        {
            return new BlockStore(BlockDirectory, Network);
        }

        public AzureIndexer CreateIndexer()
        {
            return new AzureIndexer(this);
        }

        public Node ConnectToNode()
        {
            if (String.IsNullOrEmpty(Node))
                throw new ConfigurationErrorsException("Node setting is not configured");
            return NBitcoin.Protocol.Node.Connect(Network, Node);
        }

        public string Node
        {
            get;
            set;
        }

        public Chain GetLocalChain(string name)
        {
            var path = GetFilePath(name + ".dat");
            return new Chain(Network, new StreamObjectStream<ChainChange>(File.Open(path, FileMode.OpenOrCreate)));
        }
    }


    public class AzureIndexer
    {
        public static AzureIndexer CreateIndexer()
        {
            var config = IndexerServerConfiguration.FromConfiguration();
            return config.CreateIndexer();
        }

        public int TaskCount
        {
            get;
            set;
        }

        private readonly IndexerServerConfiguration _Configuration;
        public IndexerServerConfiguration Configuration
        {
            get
            {
                return _Configuration;
            }
        }
        public AzureIndexer(IndexerServerConfiguration configuration)
        {
            if (configuration == null)
                throw new ArgumentNullException("configuration");
            CheckpointInterval = TimeSpan.FromMinutes(15.0);
            _Configuration = configuration;
            TaskCount = -1;
            FromBlk = 0;
            BlkCount = 9999999;
        }

        public TaskPool<TItem> CreateTaskPool<TItem>(BlockingCollection<TItem> collection, Action<TItem> action, int defaultTaskCount)
        {
            var pool = new TaskPool<TItem>(collection, action, defaultTaskCount)
            {
                TaskCount = TaskCount
            };
            pool.Start();
            IndexerTrace.TaskCount(pool.Tasks.Length);
            return pool;
        }

        public long IndexTransactions()
        {
            long txCount = 0;
            Helper.SetThrottling();

            BlockingCollection<TransactionEntry.Entity[]> transactions = new BlockingCollection<TransactionEntry.Entity[]>(20);

            var tasks = CreateTaskPool(transactions, (txs) => Index(txs), 30);

            using (IndexerTrace.NewCorrelation("Import transactions to azure started").Open())
            {
                Configuration.GetTransactionTable().CreateIfNotExists();
                var buckets = new MultiValueDictionary<string, TransactionEntry.Entity>();
                var storedBlocks = Enumerate("tx");
                foreach (var block in storedBlocks)
                {
                    foreach (var transaction in block.Item.Transactions)
                    {
                        txCount++;
                        var indexed = new TransactionEntry.Entity(null, transaction, block.Item.Header.GetHash());
                        buckets.Add(indexed.PartitionKey, indexed);
                        var collection = buckets[indexed.PartitionKey];
                        if (collection.Count == 100)
                        {
                            PushTransactions(buckets, collection, transactions);
                        }
                        if (storedBlocks.NeedSave)
                        {
                            foreach (var kv in buckets.AsLookup().ToArray())
                            {
                                PushTransactions(buckets, kv, transactions);
                            }
                            tasks.Stop();
                            storedBlocks.SaveCheckpoint();
                            tasks.Start();
                        }
                    }
                }

                foreach (var kv in buckets.AsLookup().ToArray())
                {
                    PushTransactions(buckets, kv, transactions);
                }
                tasks.Stop();
                storedBlocks.SaveCheckpoint();
            }
            return txCount;
        }

        private void PushTransactions(MultiValueDictionary<string, TransactionEntry.Entity> buckets,
                                        IEnumerable<TransactionEntry.Entity> indexedTransactions,
                                    BlockingCollection<TransactionEntry.Entity[]> transactions)
        {
            var array = indexedTransactions.ToArray();
            transactions.Add(array);
            buckets.Remove(array[0].PartitionKey);
        }

        TimeSpan _Timeout = TimeSpan.FromMinutes(5.0);


        public void Index(params TransactionEntry.Entity[] entities)
        {
            Index(entities.Select(e => e.CreateTableEntity()).ToArray(), Configuration.GetTransactionTable());
        }
        private void Index(IEnumerable<ITableEntity> entities, CloudTable table)
        {
            int exceptionCount = 0;
            while (true)
            {
                try
                {
                    var options = new TableRequestOptions()
                        {
                            PayloadFormat = TablePayloadFormat.Json,
                            MaximumExecutionTime = _Timeout,
                            ServerTimeout = _Timeout,
                        };

                    var batch = new TableBatchOperation();
                    int count = 0;
                    foreach (var entity in entities)
                    {
                        batch.Add(TableOperation.InsertOrReplace(entity));
                        count++;
                    }

                    if (count > 1)
                        table.ExecuteBatch(batch, options);
                    else
                    {
                        if (count == 1)
                            table.Execute(batch[0], options);
                    }

                    if (exceptionCount != 0)
                        IndexerTrace.RetryWorked();
                    break;
                }
                catch (Exception ex)
                {
                    IndexerTrace.ErrorWhileImportingEntitiesToAzure(entities.ToArray(), ex);
                    exceptionCount++;
                    if (exceptionCount > 5)
                        throw;
                    Thread.Sleep(exceptionCount * 1000);
                }
            }
        }
        public void Index(Block block)
        {
            var hash = block.GetHash().ToString();
            using (IndexerTrace.NewCorrelation("Upload of " + hash).Open())
            {
                Stopwatch watch = new Stopwatch();
                watch.Start();
                bool failedBefore = false;
                while (true)
                {
                    try
                    {
                        var container = Configuration.GetBlocksContainer();
                        var client = container.ServiceClient;
                        client.DefaultRequestOptions.SingleBlobUploadThresholdInBytes = 32 * 1024 * 1024;
                        var blob = container.GetPageBlobReference(hash);
                        MemoryStream ms = new MemoryStream();
                        block.ReadWrite(ms, true);
                        var blockBytes = ms.GetBuffer();

                        long length = 512 - (ms.Length % 512);
                        if (length == 512)
                            length = 0;
                        Array.Resize(ref blockBytes, (int)(ms.Length + length));

                        try
                        {
                            blob.UploadFromByteArray(blockBytes, 0, blockBytes.Length, new AccessCondition()
                            {
                                //Will throw if already exist, save 1 call
                                IfNotModifiedSinceTime = failedBefore ? (DateTimeOffset?)null : DateTimeOffset.MinValue
                            }, new BlobRequestOptions()
                            {
                                MaximumExecutionTime = _Timeout,
                                ServerTimeout = _Timeout
                            });
                            watch.Stop();
                            IndexerTrace.BlockUploaded(watch.Elapsed, blockBytes.Length);
                            break;
                        }
                        catch (StorageException ex)
                        {
                            var alreadyExist = ex.RequestInformation != null && ex.RequestInformation.HttpStatusCode == 412;
                            if (!alreadyExist)
                                throw;
                            watch.Stop();
                            IndexerTrace.BlockAlreadyUploaded();
                            break;
                        }
                    }
                    catch (Exception ex)
                    {
                        IndexerTrace.ErrorWhileImportingBlockToAzure(new uint256(hash), ex);
                        failedBefore = true;
                        Thread.Sleep(5000);
                    }
                }
            }
        }

        public long IndexBlocks()
        {
            long blkCount = 0;
            Helper.SetThrottling();
            BlockingCollection<Block> blocks = new BlockingCollection<Block>(20);
            var tasks = CreateTaskPool(blocks, Index, 15);

            using (IndexerTrace.NewCorrelation("Import blocks to azure started").Open())
            {
                Configuration.GetBlocksContainer().CreateIfNotExists();
                var storedBlocks = Enumerate();
                foreach (var block in storedBlocks)
                {
                    blkCount++;
                    blocks.Add(block.Item);
                    if (storedBlocks.NeedSave)
                    {
                        tasks.Stop();
                        storedBlocks.SaveCheckpoint();
                        tasks.Start();
                    }
                }
                tasks.Stop();
                storedBlocks.SaveCheckpoint();
            }
            return blkCount;
        }

        public void IndexOrderedBalances(ChainBase chain = null)
        {
            chain = chain ?? GetMainChain();
            IndexBalances("balances", (txid, tx, blockid) =>
            {
                var b = chain.GetBlock(blockid);
                var header = b == null ? null : b.Header;
                return OrderedBalanceChange.ExtractScriptBalances(txid, tx, blockid, header, b == null ? 0 : b.Height);
            });
        }

        internal ChainBase GetMainChain()
        {
            return Configuration.CreateIndexerClient().GetMainChain();
        }

        public void IndexWalletBalances(ChainBase chain = null)
        {
            chain = chain ?? GetMainChain();
            var walletRules = Configuration.CreateIndexerClient().GetAllWalletRules();
            IndexBalances("wallets", (txid, tx, blockid) =>
            {
                var b = chain.GetBlock(blockid);
                var header = b == null ? null : b.Header;
                return OrderedBalanceChange.ExtractWalletBalances(txid, tx, blockid, header, b == null ? 0 : b.Height, walletRules);
            });
        }

        private void IndexBalances(string checkpointName, Func<uint256, Transaction, uint256, IEnumerable<OrderedBalanceChange>> extract)
        {
            Helper.SetThrottling();
            BlockingCollection<OrderedBalanceChange[]> indexedEntries = new BlockingCollection<OrderedBalanceChange[]>(100);

            var tasks = CreateTaskPool(indexedEntries, (entries) => Index(entries.Select(e => e.ToEntity(Configuration.SerializerSettings)), this.Configuration.GetBalanceTable()), 30);
            using (IndexerTrace.NewCorrelation("Import balances " + checkpointName + " to azure started").Open())
            {
                this.Configuration.GetBalanceTable().CreateIfNotExists();
                var buckets = new MultiValueDictionary<string, OrderedBalanceChange>();

                var storedBlocks = Enumerate(checkpointName);
                foreach (var block in storedBlocks)
                {
                    var blockId = block.Item.Header.GetHash();
                    foreach (var tx in block.Item.Transactions)
                    {
                        var txId = tx.GetHash();
                        try
                        {
                            var entries = extract(blockId, tx, txId);

                            foreach (var entry in entries)
                            {
                                buckets.Add(entry.PartitionKey, entry);
                                var bucket = buckets[entry.PartitionKey];
                                if (bucket.Count == 100)
                                {
                                    indexedEntries.Add(bucket.ToArray());
                                    buckets.Remove(entry.PartitionKey);
                                }
                            }

                            if (storedBlocks.NeedSave)
                            {
                                foreach (var kv in buckets.AsLookup().ToArray())
                                {
                                    indexedEntries.Add(kv.ToArray());
                                }
                                buckets.Clear();
                                tasks.Stop();
                                storedBlocks.SaveCheckpoint();
                                tasks.Start();
                            }
                        }
                        catch (Exception ex)
                        {
                            IndexerTrace.ErrorWhileImportingBalancesToAzure(ex, txId);
                            throw;
                        }
                    }
                }

                foreach (var kv in buckets.AsLookup().ToArray())
                {
                    indexedEntries.Add(kv.ToArray());
                }
                tasks.Stop();
                storedBlocks.SaveCheckpoint();
            }
        }

        public void IndexOrderedBalance(int height, Block block)
        {
            var table = Configuration.GetBalanceTable();
            var blockId = block.GetHash();
            foreach (var transaction in block.Transactions)
            {
                var txId = transaction.GetHash();
                var changes = OrderedBalanceChange.ExtractScriptBalances(txId, transaction, blockId, block.Header, height).Select(c => c.ToEntity(Configuration.SerializerSettings));
                foreach (var group in changes.GroupBy(c => c.PartitionKey))
                {
                    Index(group, table);
                }
            }
        }

        public void IndexWalletOrderedBalance(int height, Block block, WalletRuleEntryCollection walletRules)
        {
            var table = Configuration.GetBalanceTable();
            var blockId = block.GetHash();
            foreach (var transaction in block.Transactions)
            {
                var txId = transaction.GetHash();
                var changes = OrderedBalanceChange.ExtractWalletBalances(txId, transaction, blockId, block.Header, height, walletRules).Select(c => c.ToEntity(Configuration.SerializerSettings));
                foreach (var group in changes.GroupBy(c => c.PartitionKey))
                {
                    Index(group, table);
                }
            }
        }

        public void IndexOrderedBalance(Transaction tx)
        {
            var table = Configuration.GetBalanceTable();
            foreach (var group in OrderedBalanceChange.ExtractScriptBalances(tx).GroupBy(c => c.BalanceId, c => c.ToEntity(Configuration.SerializerSettings)))
            {
                Index(group, table);
            }
        }

        public Chain GetNodeChain()
        {
            IndexerTrace.Information("Connecting to node " + Configuration.Node);
            using (var node = Configuration.ConnectToNode())
            {
                IndexerTrace.Information("Handshaking");
                node.VersionHandshake();
                IndexerTrace.Information("Loading local chain");
                var chain = Configuration.GetLocalChain("ImportMainChain");
                IndexerTrace.Information("Synchronizing with local node");
                node.SynchronizeChain(chain);
                IndexerTrace.Information("Chain loaded with height " + chain.Height);
                return chain;
            }
        }
        public void IndexNodeMainChain()
        {
            var chain = GetNodeChain();
            try
            {
                IndexMainChain(chain);
            }
            finally
            {
                chain.Changes.Dispose();
            }
        }


        internal const int BlockHeaderPerRow = 6;
        public void Index(ChainBase chain, int startHeight)
        {
            List<ChainPartEntry> entries = new List<ChainPartEntry>(((chain.Height - startHeight) / BlockHeaderPerRow) + 5);
            startHeight = startHeight - (startHeight % BlockHeaderPerRow);
            ChainPartEntry chainPart = null;
            for (int i = startHeight ; i <= chain.Tip.Height ; i++)
            {
                if (chainPart == null)
                    chainPart = new ChainPartEntry()
                    {
                        ChainOffset = i
                    };

                var block = chain.GetBlock(i);
                chainPart.BlockHeaders.Add(block.Header);
                if (chainPart.BlockHeaders.Count == BlockHeaderPerRow)
                {
                    entries.Add(chainPart);
                    chainPart = null;
                }
            }
            if (chainPart != null)
                entries.Add(chainPart);
            Index(entries);
        }

        private void Index(List<ChainPartEntry> chainParts)
        {
            CloudTable table = Configuration.GetChainTable();
            TableBatchOperation batch = new TableBatchOperation();
            var last = chainParts[chainParts.Count - 1];
            foreach (var entry in chainParts)
            {
                batch.Add(TableOperation.InsertOrReplace(entry.ToEntity()));
                if (batch.Count == 100)
                {
                    table.ExecuteBatch(batch);
                    batch = new TableBatchOperation();
                }
                IndexerTrace.RemainingBlockChain(entry.ChainOffset, last.ChainOffset + last.BlockHeaders.Count - 1);
            }
            if (batch.Count > 0)
            {
                table.ExecuteBatch(batch);
            }
        }



        private BlockEnumerable Enumerate(string checkpointName = null)
        {
            return new BlockEnumerable(this, checkpointName)
            {
                CheckpointInterval = CheckpointInterval
            };
        }

        public TimeSpan CheckpointInterval
        {
            get;
            set;
        }

        public int FromBlk
        {
            get;
            set;
        }

        public int BlkCount
        {
            get;
            set;
        }

        public bool NoSave
        {
            get;
            set;
        }

        public void IndexMainChain(ChainBase chain)
        {
            if (chain == null)
                throw new ArgumentNullException("chain");
            Helper.SetThrottling();

            using (IndexerTrace.NewCorrelation("Index Main chain").Open())
            {
                Configuration.GetChainTable().CreateIfNotExists();
                IndexerTrace.LocalMainChainTip(chain.Tip);
                var client = Configuration.CreateIndexerClient();
                var changes = client.GetChainChangesUntilFork(chain.Tip, true).ToList();

                var height = 0;
                if (changes.Count != 0)
                {
                    IndexerTrace.StoredMainChainTip(changes[0].BlockId, changes[0].Height);
                    if (changes[0].Height > chain.Tip.Height)
                    {
                        IndexerTrace.LocalMainChainIsLate();
                        return;
                    }
                    height = changes[changes.Count - 1].Height + 1;
                    if (height > chain.Height)
                    {
                        IndexerTrace.StoredMainChainIsUpToDate(chain.Tip);
                        return;
                    }
                }
                else
                {
                    IndexerTrace.NoForkFoundWithStored();
                }

                IndexerTrace.ImportingChain(chain.GetBlock(height), chain.Tip);
                Index(chain, height);

            }
        }




    }
}
