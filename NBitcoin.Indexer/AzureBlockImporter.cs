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
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace NBitcoin.Indexer
{
	public class AzureBlockImporter
	{
		public static AzureBlockImporter CreateBlockImporter(string progressFile = null)
		{
			if(progressFile == null)
				progressFile = "progress.dat";
			return new AzureBlockImporter(CreateBlockStore(), CreateBlobClient(), progressFile);
		}

		private static BlockStore CreateBlockStore()
		{
			return new BlockStore(ConfigurationManager.AppSettings["BlockDirectory"], Network.Main);
		}

		private static CloudBlobClient CreateBlobClient()
		{
			return new CloudBlobClient
				(
					new Uri(ConfigurationManager.AppSettings["Azure.Blob.StorageUri"]),
					new StorageCredentials(ConfigurationManager.AppSettings["Azure.Blob.AccountName"], ConfigurationManager.AppSettings["Azure.Blob.Key"])
				);
		}


		public const string Container = "nbitcoinindexer";
		private readonly CloudBlobClient _BlobClient;

		public int TaskCount
		{
			get;
			set;
		}
		public CloudBlobClient BlobClient
		{
			get
			{
				return _BlobClient;
			}
		}
		private readonly BlockStore _Store;
		public BlockStore Store
		{
			get
			{
				return _Store;
			}
		}
		private readonly string _ProgressFile;
		public string ProgressFile
		{
			get
			{
				return _ProgressFile;
			}
		}
		public AzureBlockImporter(BlockStore store, CloudBlobClient blobClient, string progressFile)
		{
			if(store == null)
				throw new ArgumentNullException("store");
			if(blobClient == null)
				throw new ArgumentNullException("blobClient");
			TaskCount = 15;
			_Store = store;
			_BlobClient = blobClient;
			_ProgressFile = progressFile;
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
		public void StartImportToAzure()
		{
			List<ImportTask> tasks = new List<ImportTask>();
			using(IndexerTrace.NewCorrelation("Import to azure started").Open())
			{
				BlobClient.GetContainerReference(Container).CreateIfNotExists();
				var startPosition = GetPosition();
				var lastPosition = startPosition;
				IndexerTrace.StartingImportAt(lastPosition);
				foreach(var block in Store.Enumerate(new DiskBlockPosRange(startPosition)))
				{
					if(tasks.Count >= TaskCount)
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
						if(tasks.Count >= TaskCount)
							Task.WaitAny(tasks.Select(t => t.Task).ToArray());
					}
					tasks.Add(Import(block));
				}
				Task.WaitAll(tasks.Select(t => t.Task).ToArray());
			}
		}

		private void SetPosition(DiskBlockPos diskBlockPos)
		{
			File.WriteAllText(ProgressFile, diskBlockPos.ToString());
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
							while(true)
							{
								try
								{
									var client = Clone(BlobClient);
									client.DefaultRequestOptions.SingleBlobUploadThresholdInBytes = 32 * 1024 * 1024;
									var container = client.GetContainerReference(Container);
									var blob = container.GetPageBlobReference(hash);
									MemoryStream ms = new MemoryStream();
									block.ReadWrite(ms, true);
									var blockBytes = ms.GetBuffer();
									if(blockBytes.Length % 512 != 0)
									{
										int length = 512 - (int)(blockBytes.Length % 512);
										Array.Resize(ref blockBytes, blockBytes.Length + length);
									}
									try
									{
										blob.UploadFromByteArray(blockBytes, 0, blockBytes.Length, new AccessCondition()
										{
											//Will throw if already exist, save 1 call
											IfNotModifiedSinceTime = DateTimeOffset.MinValue
										}, new BlobRequestOptions()
										{
											MaximumExecutionTime = TimeSpan.FromSeconds(10.0),
											ServerTimeout = TimeSpan.FromSeconds(10.0)
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
									Thread.Sleep(5000);
								}
							}
						}
					}, TaskCreationOptions.LongRunning)
				};
		}

		private CloudBlobClient Clone(CloudBlobClient client)
		{
			return new CloudBlobClient(client.BaseUri, client.Credentials);
		}

		private DiskBlockPos GetPosition()
		{
			try
			{
				return DiskBlockPos.Parse(File.ReadAllText(_ProgressFile));
			}
			catch
			{
			}
			return new DiskBlockPos(0, 0);
		}
	}
}
