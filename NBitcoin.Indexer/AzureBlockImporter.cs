using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Blob;
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
			return new Uri(String.Format("https://{0}.{1}.core.windows.net/", StorageCredentials.AccountName, clientType), UriKind.Absolute);
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
		}



		class ImportTask
		{
			public Task Task
			{
				get;
				set;
			}
			public DiskBlockPos Position
			{
				get;
				set;
			}
		}
		public void StartBlockImportToAzure()
		{
			SetThrottling();
			var blobClient = Configuration.CreateBlobClient();
			var store = Configuration.CreateStoreBlock();
			List<ImportTask> tasks = new List<ImportTask>();
			using(IndexerTrace.NewCorrelation("Import to azure started").Open())
			{
				blobClient.GetContainerReference(Configuration.Container).CreateIfNotExists();
				var startPosition = GetPosition();
				var lastPosition = startPosition;
				IndexerTrace.StartingImportAt(lastPosition);
				foreach(var block in store.Enumerate(new DiskBlockPosRange(startPosition)))
				{
					if(tasks.Count >= TaskCount)
					{
						CleanDone(tasks);
						if(tasks.Count >= TaskCount)
							Task.WaitAny(tasks.Select(t => t.Task).ToArray());
					}
					tasks.Add(Import(block));
				}
				Task.WaitAll(tasks.Select(t => t.Task).ToArray());
				CleanDone(tasks);
			}
		}

		private static void SetThrottling()
		{
			ServicePointManager.UseNagleAlgorithm = false;
			ServicePointManager.Expect100Continue = false;
			ServicePointManager.DefaultConnectionLimit = 100;
		}

		private void CleanDone(List<ImportTask> tasks)
		{
			foreach(var task in tasks.ToList())
			{
				if(task.Task.IsCompleted)
				{
					tasks.Remove(task);
					if(!tasks.Any(t => t.Position < task.Position))
					{
						SetPosition(task.Position);
						IndexerTrace.PositionSaved(task.Position);
					}
				}
			}
		}

		private void SetPosition(DiskBlockPos diskBlockPos)
		{
			File.WriteAllText(Configuration.ProgressFile, diskBlockPos.ToString());
		}

		private ImportTask Import(StoredBlock storedBlock)
		{
			var block = storedBlock.Item;
			return
				new ImportTask()
				{
					Position = storedBlock.BlockPosition,
					Task = Task.Factory.StartNew(() =>
					{
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
					}, TaskCreationOptions.LongRunning)
				};
		}

		
		private DiskBlockPos GetPosition()
		{
			try
			{
				return DiskBlockPos.Parse(File.ReadAllText(Configuration.ProgressFile));
			}
			catch
			{
			}
			return new DiskBlockPos(0, 0);
		}

		public void StartTransactionImportToAzure()
		{
			SetThrottling();
		}
	}
}
