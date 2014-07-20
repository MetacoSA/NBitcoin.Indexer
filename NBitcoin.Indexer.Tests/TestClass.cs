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
			using(var tester = CreateTester())
			{
				tester.Importer.TaskCount = 15;
				tester.Importer.BlkCount = 1;
				tester.Importer.FromBlk = 0;
				tester.Importer.StartBlockImportToAzure();
			}
		}
		[Fact]
		public void CanUploadTransactionsToAzure()
		{
			using(var tester = CreateTester())
			{

				tester.Importer.TaskCount = 15;
				tester.Importer.BlkCount = 1;
				tester.Importer.FromBlk = 0;
				tester.Importer.StartTransactionImportToAzure();
			}
		}

		private ImporterTester CreateTester([CallerMemberName]string folder = null)
		{
			return new ImporterTester(folder);
		}
	}
}
