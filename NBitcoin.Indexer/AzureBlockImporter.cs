using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Blob;
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
			TaskCount = 10;
			_Store = store;
			_BlobClient = blobClient;
			_ProgressFile = progressFile;
		}

		List<Task> _Tasks = new List<Task>();

		public void StartImportToAzure()
		{
			using(IndexerTrace.NewCorrelation("Import to azure started").Open())
			{
				BlobClient.GetContainerReference(Container).CreateIfNotExists();
				var startPosition = GetPosition();
				foreach(var block in Store.Enumerate(new DiskBlockPosRange(startPosition)))
				{
					if(_Tasks.Count >= TaskCount)
					{
						foreach(var task in _Tasks.ToList())
						{
							if(task.IsFaulted || task.IsCompleted)
								_Tasks.Remove(task);
						}
						if(_Tasks.Count >= TaskCount)
							Task.WaitAny(_Tasks.ToArray());
					}
					_Tasks.Add(Import(block));
				}
				Task.WaitAll(_Tasks.ToArray());
			}
		}

		private Task Import(StoredBlock storedBlock)
		{
			var block = storedBlock.Item;
			return
				Task.Factory.StartNew(() =>
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
								if(ms.Length % 512 != 0)
								{
									int length = 512 - (int)(ms.Length % 512);
									ms.Write(new byte[length], 0, length);
								}
								var blockBytes = ms.GetBuffer();
								try
								{
									blob.UploadFromByteArray(blockBytes, 0, blockBytes.Length, new AccessCondition()
									{
										//Will throw if already exist, save 1 call
										IfNotModifiedSinceTime = DateTimeOffset.MinValue
									});
									watch.Stop();
									IndexerTrace.BlockUploaded(watch.Elapsed, blockBytes.Length);
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
				}, TaskCreationOptions.LongRunning);
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
			return null;
		}
	}
}
