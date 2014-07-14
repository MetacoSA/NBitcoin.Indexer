using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace NBitcoin.Indexer.Tests
{
	public class TestClass
	{
		[Fact]
		public void CanUploadBlobDirectoryToAzure()
		{
			AzureBlockImporter blockImporter = CreateBlockImporter();
			blockImporter.TaskCount = 5;
			blockImporter.StartImportToAzure();
		}

		private AzureBlockImporter CreateBlockImporter([CallerMemberName]string folder = null)
		{
			TestUtils.EnsureNew(folder);
			return new AzureBlockImporter(CreateBlockStore(), CreateBlobClient(), folder + "/progress");
		}

		private BlockStore CreateBlockStore()
		{
			return new BlockStore(ConfigurationManager.AppSettings["BlockDirectory"], Network.Main);
		}

		private CloudBlobClient CreateBlobClient()
		{
			return new CloudBlobClient
				(
					new Uri(ConfigurationManager.AppSettings["Azure.Blob.StorageUri"]),
					new StorageCredentials(ConfigurationManager.AppSettings["Azure.Blob.AccountName"], ConfigurationManager.AppSettings["Azure.Blob.Key"])
				);
		}
	}
}
