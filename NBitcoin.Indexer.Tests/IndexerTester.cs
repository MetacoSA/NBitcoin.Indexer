using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Table;
using NBitcoin.Protocol;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NBitcoin.Indexer.Tests
{
	class IndexerTester : IDisposable
	{
		private readonly AzureIndexer _Importer;
		public AzureIndexer Indexer
		{
			get
			{
				return _Importer;
			}
		}
		string _Folder;
		public IndexerTester(string folder)
		{
			TestUtils.EnsureNew(folder);
			var config = IndexerServerConfiguration.FromConfiguration();
			config.BlockDirectory = "../../Data/blocks";
			config.StorageNamespace = folder;
			config.MainDirectory = folder;
			_Importer = config.CreateIndexer();


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
			if(_NodeServer != null)
				_NodeServer.Dispose();
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
				var container =  _Importer.Configuration.GetBlocksContainer();
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
				Indexer.NoSave = true;
				Indexer.TaskCount = 15;
				Indexer.BlkCount = 1;
				Indexer.FromBlk = 0;
				Indexer.IndexBlocks();
			}
		}

		internal void ImportCachedTransactions()
		{
			if(Client.GetTransaction(KnownTransactionId) == null)
			{
				Indexer.NoSave = true;
				Indexer.TaskCount = 15;
				Indexer.BlkCount = 1;
				Indexer.FromBlk = 0;
				Indexer.IndexTransactions();
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
					_Client = Indexer.Configuration.CreateIndexerClient();
				}
				return _Client;
			}
		}

		NodeServer _NodeServer;
		internal MiniNode CreateLocalNode()
		{
			NodeServer nodeServer = new NodeServer(Network.Main, internalPort: (ushort)RandomUtils.GetInt32());
			nodeServer.Listen();
			_NodeServer = nodeServer;
			Indexer.Configuration.Node = "localhost:" + nodeServer.LocalEndpoint.Port;
			var store = CreateLocalBlockStore();
			Indexer.Configuration.BlockDirectory = store.Folder.FullName;
			return new MiniNode(store, nodeServer);
		}
	}
}
