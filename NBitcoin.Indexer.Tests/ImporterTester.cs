using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NBitcoin.Indexer.Tests
{
	class ImporterTester : IDisposable
	{
		private readonly AzureBlockImporter _Importer;
		public AzureBlockImporter Importer
		{
			get
			{
				return _Importer;
			}
		}

		public ImporterTester(string folder)
		{
			TestUtils.EnsureNew(folder);
			var config = ImporterConfiguration.FromConfiguration();
			config.ProgressFile = folder + "/progress";
			config.BlockDirectory = "../../Data/blocks";
			config.TransactionTable = folder;
			config.Container = folder;

			_Importer = config.CreateImporter();
			GetTransactionTable().CreateIfNotExists();

			config.GetBlocksContainer().CreateIfNotExists();
		}



		#region IDisposable Members

		public void Dispose()
		{
			if(!Cached)
			{
				var table = GetTransactionTable();
				var entities = table.ExecuteQuery(new TableQuery()).ToList();
				Parallel.ForEach(entities, e =>
				{
					table.Execute(TableOperation.Delete(e));
				});

				var client = _Importer.Configuration.CreateBlobClient();
				var container = client.GetContainerReference(_Importer.Configuration.Container);
				var blobs = container.ListBlobs().ToList();

				Parallel.ForEach(blobs, b =>
				{
					((CloudPageBlob)b).Delete();
				});
			}
		}

		private CloudTable GetTransactionTable()
		{
			var client = _Importer.Configuration.CreateTableClient();
			var table = client.GetTableReference(_Importer.Configuration.TransactionTable);
			table.CreateIfNotExists();
			return table;
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
