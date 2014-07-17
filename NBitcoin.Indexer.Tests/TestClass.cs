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
			blockImporter.StartBlockImportToAzure();
		}
		[Fact]
		public void CanUploadTransactionsToAzure()
		{
			AzureBlockImporter blockImporter = CreateBlockImporter();
			blockImporter.TaskCount = 5;
			blockImporter.StartTransactionImportToAzure();
		}

		private AzureBlockImporter CreateBlockImporter([CallerMemberName]string folder = null)
		{
			TestUtils.EnsureNew(folder);
			var config = ImporterConfiguration.FromConfiguration();
			config.ProgressFile = folder + "/progress";
			return config.CreateImporter();
		}
	}
}
