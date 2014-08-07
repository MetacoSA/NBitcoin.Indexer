using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Blob;
using NBitcoin.DataEncoders;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
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
		public void CanSpreadBytes()
		{
			var actual = Helper.Concat(new byte[] { 1, 2, 3 }, new byte[] { 4, 5, 6 }, null, null);
			var expected = new byte[] { 1, 2, 3, 4, 5, 6 };
			Assert.True(actual.SequenceEqual(expected));

			byte[] a = null;
			byte[] b = null;
			byte[] c = null;
			byte[] d = null;

			Helper.Spread(new byte[] { 1, 2, 3, 4, 5, 6, 7 }, 3, ref a, ref b, ref c, ref d);
			Assert.True(a.SequenceEqual(new byte[] { 1, 2, 3 }));
			Assert.True(b.SequenceEqual(new byte[] { 4, 5, 6 }));
			Assert.True(c.SequenceEqual(new byte[] { 7 }));
			Assert.Null(d);
		}

		TransactionSignature sig = new TransactionSignature(Encoders.Hex.DecodeData("304602210095050cbad0bc3bad2436a651810e83f21afb1cdf75d74a13049114958942067d02210099b591d52665597fd88c4a205fe3ef82715e5a125e0f2ae736bf64dc634fba9f01"));
		[Fact]
		public void CanUploadAddressesToAzure()
		{
			using(var tester = CreateTester())
			{
				var store = tester.CreateLocalBlockStore();
				var sender = new Key().PubKey;
				var receiver = new Key().PubKey;
				var b1 = new Block()
				{
					Header =
					{
						Nonce = RandomUtils.GetUInt32()
					},
					Transactions =
					{
						new Transaction()
						{
							Outputs = 
							{
								new TxOut("10.0",sender.GetAddress(Network.Main))
							}
						}
					}
				};
				store.Append(b1);

				var b2 = new Block()
				{
					Header =
					{
						Nonce = RandomUtils.GetUInt32()
					},
					Transactions =
					{
						new Transaction()
						{
							Inputs = 
							{
								new TxIn(new OutPoint(b1.Transactions[0].GetHash(),0))
								{
									ScriptSig = new PayToPubkeyHashTemplate()
												.GenerateScriptSig(sig,sender)
								}
							},
							Outputs = 
							{
								new TxOut("2.0",receiver.GetAddress(Network.Main)),
								new TxOut("8.0",sender.GetAddress(Network.Main))
							}
						}
					}
				};
				store.Append(b2);

				tester.Importer.Configuration.BlockDirectory = store.Folder.FullName;
				tester.Importer.TaskCount = 15;

				tester.Importer.StartBlockImportToAzure();
				tester.Importer.StartTransactionImportToAzure();
				tester.Importer.StartAddressImportToAzure();

				var entries = tester.Client.GetEntries(sender);
				Assert.Equal(2, entries.Length);
				var entry = AssertContainsMoney("10.0", entries);
				Assert.Equal(entry.BlockIds[0], b1.GetHash());

				entry = AssertContainsMoney("-2.0", entries);
				Assert.NotNull(entry.Spent);
				Assert.Equal(1, entry.Spent.Count);
				Assert.Equal(b1.Transactions[0].GetHash(), entry.Spent[0].Hash);
				Assert.Equal(0, (int)entry.Spent[0].N);

				entries = tester.Client.GetEntries(receiver);
				Assert.Equal(1, entries.Length);
				AssertContainsMoney("2.0", entries);
				entries = tester.Client.GetEntries(receiver);

				var b3 = new Block()
				{
					Header =
					{
						Nonce = RandomUtils.GetUInt32()
					},
					Transactions =
					{
						new Transaction()
						{
							Inputs = 
							{
								new TxIn(new OutPoint(new uint256("bf6b530a4fd7fb107f52a8c433bc10e9388d129a6bb26567685e8b0674a76a2a"),0))
								{
									ScriptSig = new PayToPubkeyHashTemplate()
												.GenerateScriptSig(sig,sender)
								}
							},
							Outputs = 
							{
								new TxOut("2.1",receiver.GetAddress(Network.Main)),
								new TxOut("8.0",sender.GetAddress(Network.Main))
							}
						}
					}
				};
				store.Append(b3);

				tester.Importer.StartBlockImportToAzure();
				tester.Importer.StartTransactionImportToAzure();
				tester.Importer.StartAddressImportToAzure();

				entries = tester.Client.GetEntries(receiver);
				AssertContainsMoney("2.1", entries);

				entries = tester.Client.GetEntries(sender);
				AssertContainsMoney(null, entries);

				var b4 = new Block()
				{
					Header =
					{
						Nonce = RandomUtils.GetUInt32()
					},
					Transactions =
					{
						new Transaction()
						{
							Inputs = 
							{
								new TxIn(new OutPoint(b3.Transactions[0].GetHash(),0))
								{
									ScriptSig = new PayToPubkeyHashTemplate()
												.GenerateScriptSig(sig,receiver)
								}
							},
							Outputs = 
							{
								new TxOut("1.5",sender.GetAddress(Network.Main)),
							}
						}
					}
				};
				store.Append(b4);

				tester.Importer.StartBlockImportToAzure();
				tester.Importer.StartTransactionImportToAzure();
				tester.Importer.StartAddressImportToAzure();

				var tx = tester.Client.GetTransaction(false, b4.Transactions[0].GetHash());
				Assert.Null(tx.SpentTxOuts);

				tx = tester.Client.GetTransaction(true, b4.Transactions[0].GetHash());
				Assert.NotNull(tx.SpentTxOuts);
				Assert.Equal(Money.Parse("0.60"), tx.Fees);
				Assert.True(tx.SpentTxOuts[0].ToBytes().SequenceEqual(b3.Transactions[0].Outputs[0].ToBytes()));
				tx = tester.Client.GetTransaction(false, b4.Transactions[0].GetHash());
				Assert.NotNull(tx);
			}
		}

		[DebuggerHidden]
		private AddressEntry AssertContainsMoney(Money expected, AddressEntry[] entries)
		{
			var entry = entries.FirstOrDefault(e => e.BalanceChange == expected);
			Assert.True(entry != null);
			return entry;
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
