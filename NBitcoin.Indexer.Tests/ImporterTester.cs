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
			_Importer = config.CreateImporter();
			GetTransactionTable().CreateIfNotExists();

			var client = _Importer.Configuration.CreateBlobClient();
			var container = client.GetContainerReference(_Importer.Configuration.Container);
			container.CreateIfNotExists();
		}



		#region IDisposable Members

		public void Dispose()
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

		private CloudTable GetTransactionTable()
		{
			var client = _Importer.Configuration.CreateTableClient();
			var table = client.GetTableReference(_Importer.Configuration.TransactionTable);
			table.CreateIfNotExists();
			return table;
		}

		#endregion
	}
}
