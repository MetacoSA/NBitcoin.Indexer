using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Table;
using NBitcoin.Crypto;
using NBitcoin.DataEncoders;
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
			config.Node = GetValue("Node", false);
			return config;
		}
		public IndexerServerConfiguration()
		{
			ProgressFile = "progress.dat";
		}
		public string ProgressFile
		{
			get;
			set;
		}
		public string BlockDirectory
		{
			get;
			set;
		}

		public BlockStore CreateStoreBlock()
		{
			return new BlockStore(BlockDirectory, Network.Main);
		}

		public AzureIndexer CreateIndexer()
		{
			return new AzureIndexer(this);
		}

		public Node GetNode()
		{
			if(String.IsNullOrEmpty(Node))
				throw new ConfigurationErrorsException("Node setting is not configured");

			var splitted = Node.Split(':');
			var port = splitted.Length == 1 ? Network.DefaultPort : int.Parse(splitted[1]);
			IPAddress address = null;
			try
			{
				address = IPAddress.Parse(splitted[0]);
			}
			catch(FormatException)
			{
				address = Dns.GetHostEntry(splitted[0]).AddressList[0];
			}
			return new NodeServer(Network).GetNodeByEndpoint(new IPEndPoint(address, port));
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
		public static AzureIndexer CreateIndexer(string progressFile = null)
		{
			var config = IndexerServerConfiguration.FromConfiguration();
			if(progressFile != null)
				config.ProgressFile = progressFile;
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
			if(configuration == null)
				throw new ArgumentNullException("configuration");
			_Configuration = configuration;
			TaskCount = -1;
			FromBlk = 0;
			BlkCount = 9999999;
		}

		public Task[] CreateTasks<TItem>(BlockingCollection<TItem> collection, Action<TItem> action, CancellationToken cancel, int defaultTaskCount)
		{

			var tasks =
				Enumerable.Range(0, TaskCount == -1 ? defaultTaskCount : TaskCount).Select(_ => Task.Factory.StartNew(() =>
			{
				try
				{
					foreach(var item in collection.GetConsumingEnumerable(cancel))
					{
						action(item);
					}
				}
				catch(OperationCanceledException)
				{
				}
			}, TaskCreationOptions.LongRunning)).ToArray();
			IndexerTrace.TaskCount(tasks.Length);
			return tasks;
		}

		public void IndexAddresses()
		{
			SetThrottling();
			BlockingCollection<AddressEntry.Entity[]> indexedEntries = new BlockingCollection<AddressEntry.Entity[]>(100);
			var stop = new CancellationTokenSource();

			var tasks = CreateTasks(indexedEntries, (entries) => SendToAzure(entries, Configuration.GetBalanceTable()), stop.Token, 30);
			using(IndexerTrace.NewCorrelation("Import transactions to azure started").Open())
			{
				Configuration.GetBalanceTable().CreateIfNotExists();
				var buckets = new MultiValueDictionary<string, AddressEntry.Entity>();

				var storedBlocks = Enumerate("balances");
				foreach(var block in storedBlocks)
				{
					var blockId = block.Item.Header.GetHash().ToString();
					foreach(var tx in block.Item.Transactions)
					{
						var txId = tx.GetHash().ToString();
						try
						{
							Dictionary<string, AddressEntry.Entity> entryByAddress = ExtractAddressEntries(blockId, tx, txId);

							foreach(var kv in entryByAddress)
							{
								buckets.Add(kv.Value.PartitionKey, kv.Value);
								var bucket = buckets[kv.Value.PartitionKey];
								if(bucket.Count == 100)
								{
									indexedEntries.Add(bucket.ToArray());
									buckets.Remove(kv.Value.PartitionKey);
								}
							}

							if(storedBlocks.NeedSave)
							{
								foreach(var kv in buckets.AsLookup().ToArray())
								{
									indexedEntries.Add(kv.ToArray());
								}
								buckets.Clear();
								WaitProcessed(indexedEntries);
								storedBlocks.SaveCheckpoint();
							}
						}
						catch(Exception ex)
						{
							IndexerTrace.ErrorWhileImportingBalancesToAzure(ex, txId);
							throw;
						}
					}
				}

				foreach(var kv in buckets.AsLookup().ToArray())
				{
					indexedEntries.Add(kv.ToArray());
				}
				buckets.Clear();
				WaitProcessed(indexedEntries);
				stop.Cancel();
				Task.WaitAll(tasks);
				storedBlocks.SaveCheckpoint();
			}
		}

		private Dictionary<string, AddressEntry.Entity> ExtractAddressEntries(string blockId, Transaction tx, string txId)
		{
			Dictionary<string, AddressEntry.Entity> entryByAddress = new Dictionary<string, AddressEntry.Entity>();
			foreach(var input in tx.Inputs)
			{
				if(tx.IsCoinBase)
					break;
				var signer = GetSigner(input.ScriptSig);
				if(signer != null)
				{
					AddressEntry.Entity entry = null;
					if(!entryByAddress.TryGetValue(signer.ToString(), out entry))
					{
						entry = new AddressEntry.Entity(txId, signer, blockId);
						entryByAddress.Add(signer.ToString(), entry);
					}
					entry.AddSend(input.PrevOut);
				}
			}

			int i = 0;
			foreach(var output in tx.Outputs)
			{
				var receiver = GetReciever(output.ScriptPubKey);
				if(receiver != null)
				{
					AddressEntry.Entity entry = null;
					if(!entryByAddress.TryGetValue(receiver.ToString(), out entry))
					{
						entry = new AddressEntry.Entity(txId, receiver, blockId);
						entryByAddress.Add(receiver.ToString(), entry);
					}
					entry.AddReceive(i);
				}
				i++;
			}
			foreach(var kv in entryByAddress)
				kv.Value.Flush();
			return entryByAddress;
		}

		private BitcoinAddress GetReciever(Script scriptPubKey)
		{
			var payToHash = payToPubkeyHash.ExtractScriptPubKeyParameters(scriptPubKey);
			if(payToHash != null)
			{
				return new BitcoinAddress(payToHash, Configuration.Network);
			}

			var payToScript = payToScriptHash.ExtractScriptPubKeyParameters(scriptPubKey);
			if(payToScript != null)
			{
				return new BitcoinScriptAddress(payToScript, Configuration.Network);
			}
			return null;
		}





		PayToPubkeyHashTemplate payToPubkeyHash = new PayToPubkeyHashTemplate();
		PayToScriptHashTemplate payToScriptHash = new PayToScriptHashTemplate();
		private BitcoinAddress GetSigner(Script scriptSig)
		{
			var pubKey = payToPubkeyHash.ExtractScriptSigParameters(scriptSig);
			if(pubKey != null)
			{
				return new BitcoinAddress(pubKey.PublicKey.ID, Configuration.Network);
			}
			var p2sh = payToScriptHash.ExtractScriptSigParameters(scriptSig);
			if(p2sh != null)
			{
				return new BitcoinScriptAddress(p2sh.RedeemScript.ID, Configuration.Network);
			}
			return null;
		}


		public void IndexTransactions()
		{
			SetThrottling();

			BlockingCollection<IndexedTransactionEntry.Entity[]> transactions = new BlockingCollection<IndexedTransactionEntry.Entity[]>(20);

			var stop = new CancellationTokenSource();
			var tasks = CreateTasks(transactions, (txs) => SendToAzure(txs, Configuration.GetTransactionTable()), stop.Token, 30);

			using(IndexerTrace.NewCorrelation("Import transactions to azure started").Open())
			{
				Configuration.GetTransactionTable().CreateIfNotExists();
				var buckets = new MultiValueDictionary<ushort, IndexedTransactionEntry.Entity>();
				var storedBlocks = Enumerate("tx");
				foreach(var block in storedBlocks)
				{
					foreach(var transaction in block.Item.Transactions)
					{
						var indexed = new IndexedTransactionEntry.Entity(transaction, block.Item.Header.GetHash());
						buckets.Add(indexed.Key, indexed);
						var collection = buckets[indexed.Key];
						if(collection.Count == 100)
						{
							PushTransactions(buckets, collection, transactions);
						}
						if(storedBlocks.NeedSave)
						{
							foreach(var kv in buckets.AsLookup().ToArray())
							{
								PushTransactions(buckets, kv, transactions);
							}
							WaitProcessed(transactions);
							storedBlocks.SaveCheckpoint();
						}
					}
				}

				foreach(var kv in buckets.AsLookup().ToArray())
				{
					PushTransactions(buckets, kv, transactions);
				}
				WaitProcessed(transactions);
				stop.Cancel();
				Task.WaitAll(tasks);
				storedBlocks.SaveCheckpoint();
			}
		}

		private void PushTransactions(MultiValueDictionary<ushort, IndexedTransactionEntry.Entity> buckets,
										IEnumerable<IndexedTransactionEntry.Entity> indexedTransactions,
									BlockingCollection<IndexedTransactionEntry.Entity[]> transactions)
		{
			var array = indexedTransactions.ToArray();
			transactions.Add(array);
			buckets.Remove(array[0].Key);
		}

		TimeSpan _Timeout = TimeSpan.FromMinutes(5.0);

		private void SendToAzure(TableEntity[] entities, CloudTable table)
		{
			if(entities.Length == 0)
				return;
			bool firstException = false;
			while(true)
			{
				try
				{
					var options = new TableRequestOptions()
						{
							PayloadFormat = TablePayloadFormat.Json,
							MaximumExecutionTime = _Timeout,
							ServerTimeout = _Timeout,
						};
					if(entities.Length > 1)
					{
						var batch = new TableBatchOperation();
						foreach(var tx in entities)
						{
							batch.Add(TableOperation.InsertOrReplace(tx));
						}
						table.ExecuteBatch(batch, options);
					}
					else
					{
						table.Execute(TableOperation.InsertOrReplace(entities[0]), options);
					}
					if(firstException)
						IndexerTrace.RetryWorked();
					break;
				}
				catch(Exception ex)
				{
					IndexerTrace.ErrorWhileImportingEntitiesToAzure(entities, ex);
					Thread.Sleep(5000);
					firstException = true;
				}
			}
		}


		public void IndexBlocks()
		{
			SetThrottling();
			BlockingCollection<StoredBlock> blocks = new BlockingCollection<StoredBlock>(20);
			var stop = new CancellationTokenSource();
			var tasks = CreateTasks(blocks, SendToAzure, stop.Token, 15);

			using(IndexerTrace.NewCorrelation("Import blocks to azure started").Open())
			{
				Configuration.GetBlocksContainer().CreateIfNotExists();
				var storedBlocks = Enumerate();
				foreach(var block in storedBlocks)
				{
					blocks.Add(block);
					if(storedBlocks.NeedSave)
					{
						WaitProcessed(blocks);
						storedBlocks.SaveCheckpoint();
					}
				}
				WaitProcessed(blocks);
				stop.Cancel();
				Task.WaitAll(tasks);
				storedBlocks.SaveCheckpoint();
			}
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
			using(IndexerTrace.NewCorrelation("Index Mempool").Open())
			{
				var table = Configuration.GetTransactionTable();
				table.CreateIfNotExists();
				var node = Configuration.GetNode();
				try
				{


					var lastUploadedFile = new FileInfo(Configuration.GetFilePath("MempoolUploaded.txt"));
					if(!lastUploadedFile.Exists)
						lastUploadedFile.Create().Close();

					Dictionary<string, MempoolUpload> lastUploadedById = new Dictionary<string, MempoolUpload>();
					MempoolUpload[] lastUploaded = new MempoolUpload[0];
					try
					{
						lastUploaded = JsonConvert.DeserializeObject<MempoolUpload[]>(File.ReadAllText(lastUploadedFile.FullName));
						if(lastUploaded != null)
						{
							lastUploaded = lastUploaded.Where(u => !u.IsExpired)
										.ToArray();
							lastUploadedById = lastUploaded
										.ToDictionary(t => t.TxId);
						}
						else
							lastUploaded = new MempoolUpload[0];
					}

					catch(FileNotFoundException)
					{
					}
					catch(FormatException)
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
						SendToAzure(new[] { new IndexedTransactionEntry.Entity(tx) }, Configuration.GetTransactionTable());
						foreach(var kv in ExtractAddressEntries(null, tx, tx.GetHash().ToString()))
						{
							SendToAzure(new AddressEntry.Entity[] { kv.Value }, Configuration.GetBalanceTable());
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
				finally
				{
					node.Disconnect();
					node.NodeServer.Dispose();
				}
			}
			return added;
		}

		public void IndexMainChain()
		{
			SetThrottling();

			using(IndexerTrace.NewCorrelation("Index Main chain").Open())
			{
				var table = Configuration.GetChainTable();
				table.CreateIfNotExists();
				var node = Configuration.GetNode();
				var chain = Configuration.GetLocalChain("ImportMainChain");
				try
				{
					node.SynchronizeChain(chain);
					IndexerTrace.LocalMainChainTip(chain.Tip);
					var client = Configuration.CreateIndexerClient();
					var changes = client.GetChainChangesUntilFork(chain.Tip, true).ToList();

					var height = 0;
					if(changes.Count != 0)
					{
						IndexerTrace.RemoteMainChainTip(changes[0].BlockId, changes[0].Height);
						if(changes[0].Height > chain.Tip.Height)
						{
							IndexerTrace.LocalMainChainIsLate();
							return;
						}
						height = changes[changes.Count - 1].Height + 1;
						if(height > chain.Height)
						{
							IndexerTrace.LocalMainChainIsUpToDate(chain.Tip);
							return;
						}
					}

					IndexerTrace.ImportingChain(chain.GetBlock(height), chain.Tip);


					string lastPartition = null;
					TableBatchOperation batch = new TableBatchOperation();
					for(int i = height ; i <= chain.Tip.Height ; i++)
					{
						var block = chain.GetBlock(i);
						var entry = new ChainChangeEntry()
						{
							BlockId = block.HashBlock,
							Header = block.Header,
							Height = block.Height
						};
						var partition = ChainChangeEntry.Entity.GetPartitionKey(entry.Height);
						if((partition == lastPartition || lastPartition == null) && batch.Count < 100)
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
						IndexerTrace.RemainingBlockChain(i, chain.Tip.Height);
					}
					if(batch.Count > 0)
					{
						table.ExecuteBatch(batch);
					}
				}
				finally
				{
					chain.Changes.Dispose();
					node.Disconnect();
					node.NodeServer.Dispose();
				}
			}
		}



		private BlockEnumerable Enumerate(string checkpointName = null)
		{
			return new BlockEnumerable(this, checkpointName);
		}



		private void WaitProcessed<T>(BlockingCollection<T> collection)
		{
			while(collection.Count != 0)
			{
				Thread.Sleep(1000);
			}
		}

		private void SendToAzure(StoredBlock storedBlock)
		{
			var block = storedBlock.Item;
			var hash = block.GetHash().ToString();
			using(IndexerTrace.NewCorrelation("Upload of " + hash).Open())
			{
				Stopwatch watch = new Stopwatch();
				watch.Start();
				bool failedBefore = false;
				while(true)
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
						if(length == 512)
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
						catch(StorageException ex)
						{
							var alreadyExist = ex.RequestInformation != null && ex.RequestInformation.HttpStatusCode == 412;
							if(!alreadyExist)
								throw;
							watch.Stop();
							IndexerTrace.BlockAlreadyUploaded();
							break;
						}
					}
					catch(Exception ex)
					{
						IndexerTrace.ErrorWhileImportingBlockToAzure(new uint256(hash), ex);
						failedBefore = true;
						Thread.Sleep(5000);
					}
				}
			}
		}

		private static void SetThrottling()
		{
			ServicePointManager.UseNagleAlgorithm = false;
			ServicePointManager.Expect100Continue = false;
			ServicePointManager.DefaultConnectionLimit = 100;
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
