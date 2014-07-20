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
	public class ImporterConfiguration
	{
		public static ImporterConfiguration FromConfiguration()
		{
			ImporterConfiguration config = new ImporterConfiguration();
			var account = GetValue("Azure.AccountName", true);
			var key = GetValue("Azure.Key", true);
			config.StorageCredentials = new StorageCredentials(account, key);
			config.Container = GetValue("Azure.Blob.Container", false) ?? "nbitcoinindexer";
			config.BlockDirectory = GetValue("BlockDirectory", true);
			var network = GetValue("Bitcoin.Network", false) ?? "Main";
			config.Network = network.Equals("main", StringComparison.OrdinalIgnoreCase) ?
									Network.Main :
							 network.Equals("test", StringComparison.OrdinalIgnoreCase) ?
							 Network.TestNet : null;
			if(config.Network == null)
				throw new ConfigurationErrorsException("Invalid value " + network + " in appsettings (expecting Main or Test)");
			return config;
		}

		private static string GetValue(string config, bool required)
		{
			var result = ConfigurationManager.AppSettings[config];
			result = String.IsNullOrWhiteSpace(result) ? null : result;
			if(result == null && required)
				throw new ConfigurationErrorsException("AppSetting " + config + " not found");
			return result;
		}
		public ImporterConfiguration()
		{
			ProgressFile = "progress.dat";
			Network = Network.Main;
		}
		public Network Network
		{
			get;
			set;
		}
		public string BlockDirectory
		{
			get;
			set;
		}
		public string Container
		{
			get;
			set;
		}
		public string ProgressFile
		{
			get;
			set;
		}
		public StorageCredentials StorageCredentials
		{
			get;
			set;
		}
		public CloudBlobClient CreateBlobClient()
		{
			return new CloudBlobClient(MakeUri("blob"), StorageCredentials);
		}
		public BlockStore CreateStoreBlock()
		{
			return new BlockStore(BlockDirectory, Network.Main);
		}
		private Uri MakeUri(string clientType)
		{
			return new Uri(String.Format("http://{0}.{1}.core.windows.net/", StorageCredentials.AccountName, clientType), UriKind.Absolute);
		}

		public AzureBlockImporter CreateImporter()
		{
			return new AzureBlockImporter(this);
		}

		public CloudTableClient CreateTableClient()
		{
			return new CloudTableClient(MakeUri("table"), StorageCredentials);
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
			TaskCount = 15;
		}

		public void StartAddressImportToAzure()
		{
			SetThrottling();
		}

		public void StartTransactionImportToAzure()
		{
			SetThrottling();

			BlockingCollection<IndexedTransaction[]> transactions = new BlockingCollection<IndexedTransaction[]>(20);
			CancellationTokenSource stop = new CancellationTokenSource();
			var tasks =
				Enumerable.Range(0, TaskCount * 2).Select(_ => Task.Factory.StartNew(() =>
				{
					try
					{
						foreach(var tx in transactions.GetConsumingEnumerable(stop.Token))
						{
							SendToAzure(tx);
						}
					}
					catch(OperationCanceledException)
					{
					}
				}, TaskCreationOptions.LongRunning)).ToArray();

			var tableClient = Configuration.CreateTableClient();

			using(IndexerTrace.NewCorrelation("Import transactions to azure started").Open())
			{
				tableClient.GetTableReference("transactions").CreateIfNotExists();
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

		private void SendToAzure(IndexedTransaction[] transactions)
		{
			if(transactions.Length == 0)
				return;
			var client = Configuration.CreateTableClient();
			var table = client.GetTableReference("transactions");
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
						MaximumExecutionTime = TimeSpan.FromSeconds(60.0),
						ServerTimeout = TimeSpan.FromSeconds(60.0),
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
			CancellationTokenSource stop = new CancellationTokenSource();
			var tasks =
				Enumerable.Range(0, TaskCount).Select(_ => Task.Factory.StartNew(() =>
			{
				try
				{
					foreach(var block in blocks.GetConsumingEnumerable(stop.Token))
					{
						SendToAzure(block);
					}
				}
				catch(OperationCanceledException)
				{
				}
			}, TaskCreationOptions.LongRunning)).ToArray();

			var blobClient = Configuration.CreateBlobClient();
			
			using(IndexerTrace.NewCorrelation("Import blocks to azure started").Open())
			{
				blobClient.GetContainerReference(Configuration.Container).CreateIfNotExists();
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
			return new BlockEnumerable(this,checkpointName);
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
								MaximumExecutionTime = TimeSpan.FromSeconds(60.0),
								ServerTimeout = TimeSpan.FromSeconds(60.0)
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




		

	


	}
}
