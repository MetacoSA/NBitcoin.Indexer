using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NBitcoin.Indexer
{
	public class IndexerConfiguration
	{
		public static IndexerConfiguration FromConfiguration()
		{
			IndexerConfiguration config = new IndexerConfiguration();
			Fill(config);
			return config;
		}

		protected static void Fill(IndexerConfiguration config)
		{
			var account = GetValue("Azure.AccountName", true);
			var key = GetValue("Azure.Key", true);
			config.StorageCredentials = new StorageCredentials(account, key);
			config.Container = GetValue("Azure.Blob.Container", false);
			var network = GetValue("Bitcoin.Network", false) ?? "Main";
			config.Network = network.Equals("main", StringComparison.OrdinalIgnoreCase) ?
									Network.Main :
							 network.Equals("test", StringComparison.OrdinalIgnoreCase) ?
							 Network.TestNet : null;
			if(config.Network == null)
				throw new ConfigurationErrorsException("Invalid value " + network + " in appsettings (expecting Main or Test)");
		}

		protected static string GetValue(string config, bool required)
		{
			var result = ConfigurationManager.AppSettings[config];
			result = String.IsNullOrWhiteSpace(result) ? null : result;
			if(result == null && required)
				throw new ConfigurationErrorsException("AppSetting " + config + " not found");
			return result;
		}
		public IndexerConfiguration()
		{
			Network = Network.Main;
		}
		public Network Network
		{
			get;
			set;
		}

		private string _Container = "nbitcoinindexer";
		public string Container
		{
			get
			{
				return _Container;
			}
			set
			{
				_Container = value.ToLowerInvariant();
			}
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
		public IndexerClient CreateIndexerClient()
		{
			return new IndexerClient(this);
		}
		public CloudTable GetTransactionTable()
		{
			return CreateTableClient().GetTableReference(TransactionTable);
		}
		public CloudBlobContainer GetBlocksContainer()
		{
			return CreateBlobClient().GetContainerReference(TransactionTable);
		}

		private Uri MakeUri(string clientType)
		{
			return new Uri(String.Format("http://{0}.{1}.core.windows.net/", StorageCredentials.AccountName, clientType), UriKind.Absolute);
		}


		public CloudTableClient CreateTableClient()
		{
			return new CloudTableClient(MakeUri("table"), StorageCredentials);
		}

		string _TransactionTable = "transactions";
		public string TransactionTable
		{
			get
			{
				return _TransactionTable;
			}
			set
			{
				_TransactionTable = value.ToLowerInvariant();
			}
		}
	}
}
