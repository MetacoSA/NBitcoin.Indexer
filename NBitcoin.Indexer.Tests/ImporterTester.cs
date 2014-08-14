using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NBitcoin.Indexer.Tests
{
	class ImporterTester : IDisposable
	{
		private readonly AzureIndexer _Importer;
		public AzureIndexer Importer
		{
			get
			{
				return _Importer;
			}
		}
		string _Folder;
		public ImporterTester(string folder)
		{
			TestUtils.EnsureNew(folder);
			var config = IndexerServerConfiguration.FromConfiguration();
			config.ProgressFile = folder + "/progress";
			config.BlockDirectory = "../../Data/blocks";
			config.TransactionTable = folder;
			config.Container = folder;

			_Importer = config.CreateImporter();


			foreach(var table in config.EnumerateTables())
			{
				table.CreateIfNotExists();
			}

			config.GetBlocksContainer().CreateIfNotExists();
			_Folder = folder;
		}

		internal BlockStore CreateLocalBlockStore()
		{
			var dir = Directory.CreateDirectory(Path.Combine(_Folder, "blocks"));
			return new BlockStore(dir.FullName, Network.Main);
		}



		#region IDisposable Members

		public void Dispose()
		{
			if(!Cached)
			{
				foreach(var table in _Importer.Configuration.EnumerateTables())
				{
					table.CreateIfNotExists();
					var entities = table.ExecuteQuery(new TableQuery()).ToList();
					Parallel.ForEach(entities, e =>
					{
						table.Execute(TableOperation.Delete(e));
					});
				}
				var client = _Importer.Configuration.CreateBlobClient();
				var container = client.GetContainerReference(_Importer.Configuration.Container);
				var blobs = container.ListBlobs().ToList();

				Parallel.ForEach(blobs, b =>
				{
					((CloudPageBlob)b).Delete();
				});
			}
		}


		#endregion

		public bool Cached
		{
			get;
			set;
		}


		public uint256 KnownBlockId = new uint256("0000000064cc28514d6152b3c1c111424ad227fadff41da947a99535a83a824a");
		public uint256 UnknownBlockId = new uint256("0000000064cc28514d6152b3c1c111424ad227fadff41da947a99535a83a824b");

		internal void ImportCachedBlocks()
		{
			if(Client.GetBlock(KnownBlockId) == null)
			{
				Importer.NoSave = true;
				Importer.TaskCount = 15;
				Importer.BlkCount = 1;
				Importer.FromBlk = 0;
				Importer.StartBlockImportToAzure();
			}
		}

		internal void ImportCachedTransactions()
		{
			if(Client.GetTransaction(KnownTransactionId) == null)
			{
				Importer.NoSave = true;
				Importer.TaskCount = 15;
				Importer.BlkCount = 1;
				Importer.FromBlk = 0;
				Importer.StartTransactionImportToAzure();
			}
		}

		public IndexerClient _Client;
		public uint256 KnownTransactionId = new uint256("882b98507359823f93cf9830ee90e192c62d4964c16297c6dc3bf525d27a53cb");
		public uint256 UnknownTransactionId = new uint256("882b98507359823f93cf9830ee90e192c62d4964c16297c6dc3bf525d27a53cd");
		public IndexerClient Client
		{
			get
			{
				if(_Client == null)
				{
					_Client = Importer.Configuration.CreateIndexerClient();
				}
				return _Client;
			}
		}


	}
}
