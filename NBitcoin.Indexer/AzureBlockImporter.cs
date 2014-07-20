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
	public class ImporterConfiguration : IndexerConfiguration
	{
		public new static ImporterConfiguration FromConfiguration()
		{
			ImporterConfiguration config = new ImporterConfiguration();
			Fill(config);
			config.BlockDirectory = GetValue("BlockDirectory", true);
			return config;
		}
		public ImporterConfiguration()
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

		public AzureBlockImporter CreateImporter()
		{
			return new AzureBlockImporter(this);
		}
	}


	public class AzureBlockImporter
	{
		public static AzureBlockImporter CreateBlockImporter(string progressFile = null)
		{
			var config = ImporterConfiguration.FromConfiguration();
			if(progressFile != null)
				config.ProgressFile = progressFile;
			return config.CreateImporter();
		}


		public int TaskCount
		{
			get;
			set;
		}

		private readonly ImporterConfiguration _Configuration;
		public ImporterConfiguration Configuration
		{
			get
			{
				return _Configuration;
			}
		}
		public AzureBlockImporter(ImporterConfiguration configuration)
		{
			if(configuration == null)
				throw new ArgumentNullException("configuration");
			_Configuration = configuration;
			TaskCount = -1;
			FromBlk = 0;
			BlkCount = 9999999;
		}

		public void StartAddressImportToAzure()
		{
			SetThrottling();
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

		public void StartTransactionImportToAzure()
		{
			SetThrottling();

			BlockingCollection<IndexedTransaction[]> transactions = new BlockingCollection<IndexedTransaction[]>(20);

			var stop = new CancellationTokenSource();
			var tasks = CreateTasks(transactions, SendToAzure, stop.Token, 30);

			using(IndexerTrace.NewCorrelation("Import transactions to azure started").Open())
			{
				Configuration.GetTransactionTable().CreateIfNotExists();
				var buckets = new MultiDictionary<ushort, IndexedTransaction>();
				var storedBlocks = Enumerate("tx");
				foreach(var block in storedBlocks)
				{
					foreach(var transaction in block.Item.Transactions)
					{
						var indexed = new IndexedTransaction(transaction, block.Item.Header.GetHash());
						buckets.Add(indexed.Key, indexed);
						var collection = buckets[indexed.Key];
						if(collection.Count == 100)
						{
							PushTransactions(buckets, collection, transactions);
						}
						if(storedBlocks.NeedSave)
						{
							foreach(var kv in ((IEnumerable<KeyValuePair<ushort, ICollection<IndexedTransaction>>>)buckets).ToArray())
							{
								PushTransactions(buckets, kv.Value, transactions);
							}
							WaitProcessed(transactions);
							storedBlocks.SaveCheckpoint();
						}
					}
				}

				foreach(var kv in ((IEnumerable<KeyValuePair<ushort, ICollection<IndexedTransaction>>>)buckets).ToArray())
				{
					PushTransactions(buckets, kv.Value, transactions);
				}
				WaitProcessed(transactions);
				stop.Cancel();
				Task.WaitAll(tasks);
				storedBlocks.SaveCheckpoint();
			}
		}

		private void PushTransactions(MultiDictionary<ushort, IndexedTransaction> buckets,
										ICollection<IndexedTransaction> indexedTransactions,
									BlockingCollection<IndexedTransaction[]> transactions)
		{
			var array = indexedTransactions.ToArray();
			transactions.Add(array);
			buckets.Remove(array[0].Key);
		}

		TimeSpan _Timeout = TimeSpan.FromMinutes(5.0);

		private void SendToAzure(IndexedTransaction[] transactions)
		{
			if(transactions.Length == 0)
				return;
			var table = Configuration.GetTransactionTable();
			bool firstException = false;
			while(true)
			{
				var batch = new TableBatchOperation();
				try
				{
					foreach(var tx in transactions)
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
					IndexerTrace.ErrorWhileImportingTxToAzure(transactions, ex);
					Thread.Sleep(5000);
					firstException = true;
				}
			}
		}


		TimeSpan saveInterval = TimeSpan.FromMinutes(5);

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
