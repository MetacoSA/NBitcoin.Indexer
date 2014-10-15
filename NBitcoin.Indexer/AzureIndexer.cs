using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Table;
using NBitcoin.Crypto;
using NBitcoin.DataEncoders;
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
            CheckpointInterval = TimeSpan.FromMinutes(10.0);
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

        public void IndexBalances()
        {
            SetThrottling();
            BlockingCollection<AddressEntry.Entity[]> indexedEntries = new BlockingCollection<AddressEntry.Entity[]>(100);

            var tasks = CreateTaskPool(indexedEntries, (entries) => Index(entries), 30);
            using (IndexerTrace.NewCorrelation("Import balances to azure started").Open())
            {
                Configuration.GetBalanceTable().CreateIfNotExists();
                var buckets = new MultiValueDictionary<string, AddressEntry.Entity>();

                var storedBlocks = Enumerate("balances");
                foreach (var block in storedBlocks)
                {
                    var blockId = block.Item.Header.GetHash();
                    foreach (var tx in block.Item.Transactions)
                    {
                        var txId = tx.GetHash();
                        try
                        {
                            var entryByAddress = AddressEntry.Entity.ExtractFromTransaction(blockId, tx, txId);

                            foreach (var kv in entryByAddress)
                            {
                                buckets.Add(kv.Value.PartitionKey, kv.Value);
                                var bucket = buckets[kv.Value.PartitionKey];
                                if (bucket.Count == 100)
                                {
                                    indexedEntries.Add(bucket.ToArray());
                                    buckets.Remove(kv.Value.PartitionKey);
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
                buckets.Clear();
                tasks.Stop();
                storedBlocks.SaveCheckpoint();
            }
        }






        public long IndexTransactions()
        {
            long txCount = 0;
            SetThrottling();

            BlockingCollection<TransactionEntry.Entity[]> transactions = new BlockingCollection<TransactionEntry.Entity[]>(20);

            var tasks = CreateTaskPool(transactions, (txs) => Index(txs), 30);

            using (IndexerTrace.NewCorrelation("Import transactions to azure started").Open())
            {
                Configuration.GetTransactionTable().CreateIfNotExists();
                var buckets = new MultiValueDictionary<ushort, TransactionEntry.Entity>();
                var storedBlocks = Enumerate("tx");
                foreach (var block in storedBlocks)
                {
                    foreach (var transaction in block.Item.Transactions)
                    {
                        txCount++;
                        var indexed = new TransactionEntry.Entity(transaction, block.Item.Header.GetHash());
                        buckets.Add(indexed.Key, indexed);
                        var collection = buckets[indexed.Key];
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

        private void PushTransactions(MultiValueDictionary<ushort, TransactionEntry.Entity> buckets,
                                        IEnumerable<TransactionEntry.Entity> indexedTransactions,
                                    BlockingCollection<TransactionEntry.Entity[]> transactions)
        {
            var array = indexedTransactions.ToArray();
            transactions.Add(array);
            buckets.Remove(array[0].Key);
        }

        TimeSpan _Timeout = TimeSpan.FromMinutes(5.0);
        public readonly static Network InternalNetwork = Network.Main;


        public void Index(params AddressEntry.Entity[] entries)
        {
            Index(entries.Select(e => e.CreateTableEntity()).ToArray(), Configuration.GetBalanceTable());
        }
        public void Index(params TransactionEntry.Entity[] entities)
        {
            Index(entities, Configuration.GetTransactionTable());
        }
        private void Index(ITableEntity[] entities, CloudTable table)
        {
            if (entities.Length == 0)
                return;
            bool firstException = false;
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
                    if (entities.Length > 1)
                    {
                        var batch = new TableBatchOperation();
                        foreach (var tx in entities)
                        {
                            batch.Add(TableOperation.InsertOrReplace(tx));
                        }
                        table.ExecuteBatch(batch, options);
                    }
                    else
                    {
                        table.Execute(TableOperation.InsertOrReplace(entities[0]), options);
                    }
                    if (firstException)
                        IndexerTrace.RetryWorked();
                    break;
                }
                catch (Exception ex)
                {
                    IndexerTrace.ErrorWhileImportingEntitiesToAzure(entities, ex);
                    Thread.Sleep(5000);
                    firstException = true;
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
            SetThrottling();
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


        public class MempoolUpload
        {
            public string TxId
            {
                get;
                set;
            }
            public DateTimeOffset Date
            {
                get;
                set;
            }
            public TimeSpan Age
            {
                get
                {
                    return DateTimeOffset.UtcNow - Date;
                }
            }
            public bool IsExpired
            {
                get
                {
                    return Age > TimeSpan.FromHours(12);
                }
            }
        }

        public int IndexMempool()
        {
            int added = 0;
            SetThrottling();
            using (IndexerTrace.NewCorrelation("Index Mempool").Open())
            {
                var table = Configuration.GetTransactionTable();
                table.CreateIfNotExists();
                using (var node = Configuration.ConnectToNode())
                {
                    var lastUploadedFile = new FileInfo(Configuration.GetFilePath("MempoolUploaded.txt"));
                    if (!lastUploadedFile.Exists)
                        lastUploadedFile.Create().Close();

                    Dictionary<string, MempoolUpload> lastUploadedById = new Dictionary<string, MempoolUpload>();
                    MempoolUpload[] lastUploaded = new MempoolUpload[0];
                    try
                    {
                        lastUploaded = JsonConvert.DeserializeObject<MempoolUpload[]>(File.ReadAllText(lastUploadedFile.FullName));
                        if (lastUploaded != null)
                        {
                            lastUploaded = lastUploaded.Where(u => !u.IsExpired)
                                        .ToArray();
                            lastUploadedById = lastUploaded
                                        .ToDictionary(t => t.TxId);
                        }
                        else
                            lastUploaded = new MempoolUpload[0];
                    }

                    catch (FileNotFoundException)
                    {
                    }
                    catch (FormatException)
                    {
                    }

                    var txIds = node.GetMempool();
                    var txToUpload =
                        txIds
                        .Where(tx => !lastUploadedById.ContainsKey(tx.ToString()))
                        .ToArray();

                    var transactions = node.GetMempoolTransactions(txToUpload);
                    IndexerTrace.Information("Indexing " + transactions.Length + " transactions");
                    Parallel.ForEach(transactions, new ParallelOptions()
                    {
                        MaxDegreeOfParallelism = this.TaskCount
                    },
                    tx =>
                    {
                        Index(new TransactionEntry.Entity(tx));
                        foreach (var kv in AddressEntry.Entity.ExtractFromTransaction(tx, tx.GetHash()))
                        {
                            Index(new AddressEntry.Entity[] { kv.Value });
                        }
                        Interlocked.Increment(ref added);
                    });

                    var uploaded = lastUploaded.Concat(transactions.Select(tx => new MempoolUpload()
                        {
                            Date = DateTimeOffset.UtcNow,
                            TxId = tx.GetHash().ToString()
                        })).ToArray();

                    File.WriteAllText(lastUploadedFile.FullName, JsonConvert.SerializeObject(uploaded));
                    IndexerTrace.Information("Progression saved to " + lastUploadedFile.FullName);
                }
            }

            return added;
        }

        public void IndexMainChain()
        {
            SetThrottling();

            using (IndexerTrace.NewCorrelation("Index Main chain").Open())
            {
                Configuration.GetChainTable().CreateIfNotExists();
                using (var node = Configuration.ConnectToNode())
                {
                    var chain = Configuration.GetLocalChain("ImportMainChain");
                    try
                    {
                        node.SynchronizeChain(chain);
                        IndexerTrace.LocalMainChainTip(chain.Tip);
                        var client = Configuration.CreateIndexerClient();
                        var changes = client.GetChainChangesUntilFork(chain.Tip, true).ToList();

                        var height = 0;
                        if (changes.Count != 0)
                        {
                            IndexerTrace.RemoteMainChainTip(changes[0].BlockId, changes[0].Height);
                            if (changes[0].Height > chain.Tip.Height)
                            {
                                IndexerTrace.LocalMainChainIsLate();
                                return;
                            }
                            height = changes[changes.Count - 1].Height + 1;
                            if (height > chain.Height)
                            {
                                IndexerTrace.LocalMainChainIsUpToDate(chain.Tip);
                                return;
                            }
                        }

                        IndexerTrace.ImportingChain(chain.GetBlock(height), chain.Tip);
                        Index(chain, height);
                    }
                    finally
                    {
                        chain.Changes.Dispose();
                    }
                }
            }
        }

        public void Index(Chain chain, int startHeight)
        {
            List<ChainChangeEntry> entries = new List<ChainChangeEntry>();
            for (int i = startHeight ; i <= chain.Tip.Height ; i++)
            {
                var block = chain.GetBlock(i);
                var entry = new ChainChangeEntry()
                {
                    BlockId = block.HashBlock,
                    Header = block.Header,
                    Height = block.Height
                };
                entries.Add(entry);

            }

            Index(entries.ToArray());

        }

        public void Index(params ChainChangeEntry[] chainChanges)
        {
            CloudTable table = Configuration.GetChainTable();
            string lastPartition = null;
            TableBatchOperation batch = new TableBatchOperation();
            ChainChangeEntry last = chainChanges.LastOrDefault();
            for (int i = 0 ; i < chainChanges.Length ; i++)
            {
                var entry = chainChanges[i];
                var partition = ChainChangeEntry.Entity.GetPartitionKey(entry.Height);
                if ((partition == lastPartition || lastPartition == null) && batch.Count < 100)
                {
                    batch.Add(TableOperation.InsertOrReplace(entry.ToEntity()));
                }
                else
                {
                    table.ExecuteBatch(batch);
                    batch = new TableBatchOperation();
                    batch.Add(TableOperation.InsertOrReplace(entry.ToEntity()));
                }
                lastPartition = partition;
                IndexerTrace.RemainingBlockChain(entry.Height, last.Height);
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


        private static void SetThrottling()
        {
            Helper.SetThrottling();
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


    }
}
