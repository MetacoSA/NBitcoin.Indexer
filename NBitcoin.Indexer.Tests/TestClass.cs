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

		[Fact]
		public void CanUploadAddressesToAzure()
		{
			using(var tester = CreateTester())
			{
				tester.Importer.Configuration.BlockDirectory = "../../Data/blocks2";
				tester.Importer.TaskCount = 15;
				tester.Importer.BlkCount = 1;
				tester.Importer.FromBlk = 0;
				tester.Importer.StartAddressImportToAzure();
			}
		}

		[Fact]
		public void CanGetBlock()
		{
			using(var tester = CreateTester("cached"))
			{
				tester.Cached = true;
				tester.ImportCachedBlocks();

				var block = tester.Client.GetBlock(tester.KnownBlockId);
				Assert.True(block.CheckMerkleRoot());
				block = tester.Client.GetBlock(tester.UnknownBlockId);
				Assert.Null(block);
			}
		}
		[Fact]
		public void CanGetTransaction()
		{
			using(var tester = CreateTester("cached"))
			{
				tester.Cached = true;
				tester.ImportCachedBlocks();
				tester.ImportCachedTransactions();

				var tx = tester.Client.GetTransaction(tester.KnownTransactionId);
				Assert.True(tx.Transaction.GetHash() == tester.KnownTransactionId);
				Assert.True(tx.TransactionId == tester.KnownTransactionId);
				Assert.True(tx.BlockIds[0] == tester.KnownBlockId);

				tx = tester.Client.GetTransaction(tester.UnknownTransactionId);
				Assert.Null(tx);
			}
		}

		private ImporterTester CreateTester([CallerMemberName]string folder = null)
		{
			return new ImporterTester(folder);
		}
	}
}
