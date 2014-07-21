using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Table;
using NBitcoin.Crypto;
using NBitcoin.DataEncoders;
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

		public AzureIndexer CreateImporter()
		{
			return new AzureIndexer(this);
		}
	}


	public class AzureIndexer
	{
		public static AzureIndexer CreateBlockImporter(string progressFile = null)
		{
			var config = IndexerServerConfiguration.FromConfiguration();
			if(progressFile != null)
				config.ProgressFile = progressFile;
			return config.CreateImporter();
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

		public void StartAddressImportToAzure()
		{
			SetThrottling();
			BlockingCollection<IndexedAddressEntry[]> indexedEntries = new BlockingCollection<IndexedAddressEntry[]>(100);
			var stop = new CancellationTokenSource();

			var tasks = CreateTasks(indexedEntries, (entries) => SendToAzure(entries, Configuration.GetBalanceTable()), stop.Token, 30);
			using(IndexerTrace.NewCorrelation("Import transactions to azure started").Open())
			{
				Configuration.GetBalanceTable().CreateIfNotExists();
				var buckets = new MultiDictionary<string, IndexedAddressEntry>();

				var storedBlocks = Enumerate("balances");
				foreach(var block in storedBlocks)
				{
					foreach(var tx in block.Item.Transactions)
					{
						var txId = tx.GetHash().ToString();

						Dictionary<string, IndexedAddressEntry> entryByAddress = new Dictionary<string, IndexedAddressEntry>();
						foreach(var input in tx.Inputs)
						{
							var signer = GetSigner(input.ScriptSig);
							if(signer != null)
							{
								IndexedAddressEntry entry = null;
								if(!entryByAddress.TryGetValue(signer.ToString(), out entry))
								{
									entry = new IndexedAddressEntry(txId, signer);
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
								IndexedAddressEntry entry = null;
								if(!entryByAddress.TryGetValue(receiver.ToString(), out entry))
								{
									entry = new IndexedAddressEntry(txId, receiver);
									entryByAddress.Add(receiver.ToString(), entry);
								}
								entry.AddReceive(i);
							}
							i++;
						}

						foreach(var kv in entryByAddress)
						{
							var bucket = buckets[kv.Value.PartitionKey];
							bucket.Add(kv.Value);
							if(bucket.Count == 100)
							{
								indexedEntries.Add(bucket.ToArray());
								buckets.Remove(kv.Value.PartitionKey);
							}
						}

						if(storedBlocks.NeedSave)
						{
							foreach(var kv in ((IEnumerable<KeyValuePair<string, ICollection<IndexedAddressEntry>>>)buckets).ToArray())
							{
								indexedEntries.Add(kv.Value.ToArray());
							}
							buckets.Clear();
							WaitProcessed(indexedEntries);
							storedBlocks.SaveCheckpoint();
						}
					}

				}

				foreach(var kv in ((IEnumerable<KeyValuePair<string, ICollection<IndexedAddressEntry>>>)buckets).ToArray())
				{
					indexedEntries.Add(kv.Value.ToArray());
				}
				buckets.Clear();
				WaitProcessed(indexedEntries);
				stop.Cancel();
				Task.WaitAll(tasks);
				storedBlocks.SaveCheckpoint();
			}
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


		public void StartTransactionImportToAzure()
		{
			SetThrottling();

			BlockingCollection<TransactionEntity[]> transactions = new BlockingCollection<TransactionEntity[]>(20);

			var stop = new CancellationTokenSource();
			var tasks = CreateTasks(transactions, (txs) => SendToAzure(txs, Configuration.GetTransactionTable()), stop.Token, 30);

			using(IndexerTrace.NewCorrelation("Import transactions to azure started").Open())
			{
				Configuration.GetTransactionTable().CreateIfNotExists();
				var buckets = new MultiDictionary<ushort, TransactionEntity>();
				var storedBlocks = Enumerate("tx");
				foreach(var block in storedBlocks)
				{
					foreach(var transaction in block.Item.Transactions)
					{
						var indexed = new TransactionEntity(transaction, block.Item.Header.GetHash());
						buckets.Add(indexed.Key, indexed);
						var collection = buckets[indexed.Key];
						if(collection.Count == 100)
						{
							PushTransactions(buckets, collection, transactions);
						}
						if(storedBlocks.NeedSave)
						{
							foreach(var kv in ((IEnumerable<KeyValuePair<ushort, ICollection<TransactionEntity>>>)buckets).ToArray())
							{
								PushTransactions(buckets, kv.Value, transactions);
							}
							WaitProcessed(transactions);
							storedBlocks.SaveCheckpoint();
						}
					}
				}

				foreach(var kv in ((IEnumerable<KeyValuePair<ushort, ICollection<TransactionEntity>>>)buckets).ToArray())
				{
					PushTransactions(buckets, kv.Value, transactions);
				}
				WaitProcessed(transactions);
				stop.Cancel();
				Task.WaitAll(tasks);
				storedBlocks.SaveCheckpoint();
			}
		}

		private void PushTransactions(MultiDictionary<ushort, TransactionEntity> buckets,
										ICollection<TransactionEntity> indexedTransactions,
									BlockingCollection<TransactionEntity[]> transactions)
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
				var batch = new TableBatchOperation();
				try
				{
					foreach(var tx in entities)
					{
						batch.Add(TableOperation.InsertOrReplace(tx));
					}
					table.ExecuteBatch(batch, new TableRequestOptions()
					{
						PayloadFormat = TablePayloadFormat.Json,
						MaximumExecutionTime = _Timeout,
						ServerTimeout = _Timeout,
					});
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


		public void StartBlockImportToAzure()
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
						var client = Configuration.CreateBlobClient();
						client.DefaultRequestOptions.SingleBlobUploadThresholdInBytes = 32 * 1024 * 1024;
						var container = client.GetContainerReference(Configuration.Container);
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
