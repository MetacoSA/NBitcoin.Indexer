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
				tester.Indexer.TaskCount = 15;
				tester.Indexer.BlkCount = 1;
				tester.Indexer.FromBlk = 0;
				tester.Indexer.IndexBlocks();
			}
		}
		[Fact]
		public void CanUploadTransactionsToAzure()
		{
			using(var tester = CreateTester())
			{

				tester.Indexer.TaskCount = 15;
				tester.Indexer.BlkCount = 1;
				tester.Indexer.FromBlk = 0;
				tester.Indexer.IndexTransactions();
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

		[Fact]
		public void CanImportMainChain()
		{
			using(var tester = CreateTester())
			{
				var store = tester.CreateLocalBlockStore();
				tester.Indexer.Configuration.BlockDirectory = store.Folder.FullName;
				var chain = new Chain(Network.Main);

				BlockGenerator generator = new BlockGenerator(store);
				generator.Generate();
				var fork = generator.Generate();
				var firstTip = generator.Generate();
				tester.Indexer.IndexMainChain();

				var result = tester.Client.GetChainChangesUntilFork(chain, true).ToList();
				Assert.Equal(result[0].BlockId, firstTip.GetHash());
				Assert.Equal(result.Last().BlockId, chain.Tip.HashBlock);
				Assert.Equal(result.Count, 4);

				result = tester.Client.GetChainChangesUntilFork(chain, false).ToList();
				Assert.Equal(result[0].BlockId, firstTip.GetHash());
				Assert.NotEqual(result.Last().BlockId, chain.Tip.HashBlock);
				Assert.Equal(result.Count, 3);

				Assert.Equal(firstTip.GetHash(), tester.Client.GetBestBlock().BlockId);

				result.UpdateChain(chain);

				Assert.Equal(firstTip.GetHash(), chain.Tip.HashBlock);

				generator.Chain.SetTip(fork.Header);
				generator.Generate();
				generator.Generate();
				var secondTip = generator.Generate();

				tester.Indexer.IndexMainChain();
				Assert.Equal(secondTip.GetHash(), tester.Client.GetBestBlock().BlockId);

				result = tester.Client.GetChainChangesUntilFork(chain, false).ToList();
				result.UpdateChain(chain);
				Assert.Equal(secondTip.GetHash(), chain.Tip.HashBlock);

				var ultimateTip = generator.Generate(200);
				tester.Indexer.IndexMainChain();
				result = tester.Client.GetChainChangesUntilFork(chain, false).ToList();

				Assert.Equal(ultimateTip.Header.GetHash(), result[0].BlockId);
				Assert.Equal(tester.Client.GetBestBlock().BlockId, result[0].BlockId);
				result.UpdateChain(chain);
				Assert.Equal(ultimateTip.Header.GetHash(), chain.Tip.HashBlock);
			}
		}

		public List<ChainChange> SeeChainChanges(Chain chain)
		{
			chain.Changes.Rewind();
			return chain.Changes.Enumerate().ToList();
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
						Nonce = RandomUtils.GetUInt32(),
						HashPrevBlock = Network.Main.GetGenesis().GetHash()
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
						Nonce = RandomUtils.GetUInt32(),
						HashPrevBlock = b1.GetHash()
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

				tester.Indexer.Configuration.BlockDirectory = store.Folder.FullName;
				tester.Indexer.TaskCount = 15;

				tester.Indexer.IndexBlocks();
				tester.Indexer.IndexTransactions();
				tester.Indexer.IndexAddresses();

				var entries = tester.Client.GetEntries(sender);
				Assert.Equal(2, entries.Length);
				var entry = AssertContainsMoney("10.0", entries);
				Assert.True(new[] { new OutPoint(b1.Transactions[0].GetHash(), 0) }.SequenceEqual(entry.ReceivedOutpoints));
				Assert.Equal(entry.BlockIds[0], b1.GetHash());

				entry = AssertContainsMoney("-2.0", entries);
				Assert.NotNull(entry.SpentOutpoints);
				Assert.Equal(1, entry.SpentOutpoints.Count);
				Assert.Equal(b1.Transactions[0].GetHash(), entry.SpentOutpoints[0].Hash);
				Assert.Equal(0, (int)entry.SpentOutpoints[0].N);

				entries = tester.Client.GetEntries(receiver);
				Assert.Equal(1, entries.Length);
				AssertContainsMoney("2.0", entries);
				entries = tester.Client.GetEntries(receiver);

				var b3 = new Block()
				{
					Header =
					{
						Nonce = RandomUtils.GetUInt32(),
						HashPrevBlock = b2.GetHash()
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

				tester.Indexer.IndexBlocks();
				tester.Indexer.IndexTransactions();
				tester.Indexer.IndexAddresses();

				entries = tester.Client.GetEntries(receiver);
				AssertContainsMoney("2.1", entries);

				entries = tester.Client.GetEntries(sender);
				AssertContainsMoney(null, entries);

				var b4 = new Block()
				{
					Header =
					{
						Nonce = RandomUtils.GetUInt32(),
						HashPrevBlock = b3.GetHash(),
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

				tester.Indexer.IndexBlocks();
				tester.Indexer.IndexTransactions();
				tester.Indexer.IndexAddresses();

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

		private IndexerTester CreateTester([CallerMemberName]string folder = null)
		{
			return new IndexerTester(folder);
		}
	}
}
