using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
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

        public void EnsureSetup()
        {
            foreach (var table in EnumerateTables())
            {
                table.CreateIfNotExists();
            }
        }

		protected static void Fill(IndexerConfiguration config)
		{
			var account = GetValue("Azure.AccountName", true);
			var key = GetValue("Azure.Key", true);
			config.StorageCredentials = new StorageCredentials(account, key);
			config.StorageNamespace = GetValue("StorageNamespace", false);
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

		string _Container = "indexer";
		string _TransactionTable = "transactions";
		string _BalanceTable = "balances";
		string _ChainTable = "chain";
        string _WalletTable = "wallets";
        string _WalletBalanceTable = "walletbalances";

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
			return CreateTableClient().GetTableReference(GetFullName(_TransactionTable));
		}
        public CloudTable GetWalletRulesTable()
        {
            return CreateTableClient().GetTableReference(GetFullName(_WalletTable));
        }
        public CloudTable GetWalletBalanceTable()
        {
            return CreateTableClient().GetTableReference(GetFullName(_WalletBalanceTable));
        }

		private string GetFullName(string storageObjectName)
		{
			return (StorageNamespace + storageObjectName).ToLowerInvariant();
		}
		public CloudTable GetBalanceTable()
		{
			return CreateTableClient().GetTableReference(GetFullName(_BalanceTable));
		}
		public CloudTable GetChainTable()
		{
			return CreateTableClient().GetTableReference(GetFullName(_ChainTable));
		}

		public CloudBlobContainer GetBlocksContainer()
		{
			return CreateBlobClient().GetContainerReference(GetFullName(_Container));
		}

		private Uri MakeUri(string clientType)
		{
			return new Uri(String.Format("http://{0}.{1}.core.windows.net/", StorageCredentials.AccountName, clientType), UriKind.Absolute);
		}


		public CloudTableClient CreateTableClient()
		{
			return new CloudTableClient(MakeUri("table"), StorageCredentials);
		}


		public string StorageNamespace
		{
			get;
			set;
		}

		public IEnumerable<CloudTable> EnumerateTables()
		{
			yield return GetTransactionTable();
			yield return GetBalanceTable();
			yield return GetChainTable();
            yield return GetWalletRulesTable();
            yield return GetWalletBalanceTable();
		}


        internal IndexerServerConfiguration AsServer()
        {
            return new IndexerServerConfiguration()
            {
                StorageCredentials = StorageCredentials,
                StorageNamespace = StorageNamespace,
                Network = Network                
            };
        }
    }
}
