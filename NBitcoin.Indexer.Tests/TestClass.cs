using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Table;
using NBitcoin.BitcoinCore;
using NBitcoin.DataEncoders;
using NBitcoin.OpenAsset;
using NBitcoin.Protocol;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace NBitcoin.Indexer.Tests
{
    public class TestClass
    {
        [Fact]
        public void CanSerializeOrderedBalanceToEntity()
        {
            uint256 txId = new uint256(RandomUtils.GetBytes(32));
            Script script = CreateScript(512);
            OrderedBalanceChange balance = new OrderedBalanceChange(txId, script, null, null, 0);
            Assert.Equal(script, balance.ScriptPubKey);
            var entity = balance.ToEntity();
            Assert.False(entity.Properties.ContainsKey("h"));
            balance = new OrderedBalanceChange(entity);
            Assert.Equal(script, balance.ScriptPubKey);

            script = CreateScript(513);
            balance = new OrderedBalanceChange(txId, script, null, null, 0);
            Assert.Equal(script, balance.ScriptPubKey);
            entity = balance.ToEntity();
            Assert.True(entity.Properties.ContainsKey("h"));
            balance = new OrderedBalanceChange(entity);
            Assert.Equal(script, balance.ScriptPubKey);
        }

        private Script CreateScript(int size)
        {
            return new Script(Op.GetPushOp(new byte[size - 3]));
        }

        [Fact]
        public void CanSpreadBytes()
        {
            var bytes =
                Helper.SerializeList(Enumerable.Range(0, 300000).Select(e => new OrderedBalanceChange.IntCompactVarInt((uint)e)).ToArray());

            DynamicTableEntity entity = new DynamicTableEntity();
            Helper.SetEntityProperty(entity, "a", bytes);
            var actualBytes = Helper.GetEntityProperty(entity, "a");
            Assert.True(actualBytes.SequenceEqual(bytes));
        }
        [Fact]
        public void DoesNotCrashExtractingAddressFromBigTransaction()
        {
            var tx = new Transaction(Encoders.Hex.DecodeData(File.ReadAllText("Data/BigTransaction.txt")));
            var txId = tx.GetHash();
            var result = OrderedBalanceChange.ExtractScriptBalances(txId, tx, null, null, 0);
            foreach (var e in result)
            {
                var entity = e.ToEntity();
            }
        }
        [Fact]
        public void CanIndexBlocks()
        {
            using (var tester = CreateTester())
            {
                var node = tester.CreateLocalNode();
                node.ChainBuilder.Load("../../Data/blocks");

                tester.Indexer.TaskCount = 15;
                Assert.Equal(138, tester.Indexer.IndexBlocks());
                Assert.Equal(0, tester.Indexer.IndexBlocks());

                node.ChainBuilder.Generate();
                node.ChainBuilder.Generate();

                Assert.Equal(2, tester.Indexer.IndexBlocks());

                tester.Indexer.DeleteCheckpoints();

                tester.Indexer.FromHeight = 10;
                tester.Indexer.ToHeight = 12;
                Assert.Equal(3, tester.Indexer.IndexBlocks()); //10,11,12
                tester.Indexer.ToHeight = 14;
                Assert.Equal(2, tester.Indexer.IndexBlocks()); //13,14

                tester.Indexer.FromHeight = 19;
                tester.Indexer.ToHeight = 20;
                Assert.Equal(2, tester.Indexer.IndexBlocks()); //19,20

                tester.Indexer.FromHeight = 23;
                tester.Indexer.ToHeight = 25;
                Assert.Equal(3, tester.Indexer.IndexBlocks()); //23,24,25

                Assert.Equal(0, tester.Indexer.IndexBlocks()); //23,24,25

                tester.Indexer.NoSave = true;
                Assert.Equal(3, tester.Indexer.IndexBlocks()); //23,24,25
            }
        }
        [Fact]
        public void CanIndexTransactions()
        {
            using (var tester = CreateTester())
            {
                tester.CreateLocalNode().ChainBuilder.Load("../../Data/blocks");
                tester.Indexer.TaskCount = 15;
                Assert.Equal(138, tester.Indexer.IndexTransactions());
                Assert.Equal(0, tester.Indexer.IndexTransactions());
            }
        }


        //[Fact]
        //public void CanGetOrderedBalance2()
        //{

        //}

        BitcoinSecret alice = new BitcoinSecret("KyJTjvFpPF6DDX4fnT56d2eATPfxjdUPXFFUb85psnCdh34iyXRQ");
        BitcoinSecret bob = new BitcoinSecret("KysJMPCkFP4SLsEQAED9CzCurJBkeVvAa4jeN1BBtYS7P5LocUBQ");
        BitcoinSecret nico = new BitcoinSecret("L2uC8xNjmcfwje6eweucYvFsmKASbMDALy4rCJBAg8wofpH6barj");
        BitcoinSecret satoshi = new BitcoinSecret("L1CpAon5d8zroENbkiMbk3dtd3kcbms6QGF5x475KKTMmXVaJXh3");

        BitcoinSecret goldGuy = new BitcoinSecret("KyuzoVnpsqW529yzozkzP629wUDBsPmm4QEkh9iKnvw3Dy5JJiNg");
        BitcoinSecret silverGuy = new BitcoinSecret("L4KvjpqDtdGEn7Lw6HdDQjbg74MwWRrFZMQTgJozeHAKJw5rQ2Kn");

        [Fact]
        public void CanGetColoredBalance()
        {
            using (var tester = CreateTester())
            {
                var chainBuilder = tester.CreateChainBuilder();
                tester.Client.ColoredBalance = true;

                //Colored coin Payment
                //GoldGuy emits gold to Nico
                var txBuilder = new TransactionBuilder();

                var issuanceCoinsTransaction
                    = new Transaction()
                    {
                        Outputs =
                        {
                            new TxOut("1.0", goldGuy.PrivateKey.PubKey),
                            new TxOut("1.0", silverGuy.PrivateKey.PubKey),
                            new TxOut("1.0", nico.GetAddress()),
                            new TxOut("1.0", alice.GetAddress()),
                        }
                    };

                IssuanceCoin[] issuanceCoins = issuanceCoinsTransaction
                                        .Outputs
                                        .Take(2)
                                        .Select((o, i) => new Coin(new OutPoint(issuanceCoinsTransaction.GetHash(), i), o))
                                        .Select(c => new IssuanceCoin(c))
                                        .ToArray();
                var goldIssuanceCoin = issuanceCoins[0];
                var silverIssuanceCoin = issuanceCoins[1];
                var nicoCoin = new Coin(new OutPoint(issuanceCoinsTransaction, 2), issuanceCoinsTransaction.Outputs[2]);
                var aliceCoin = new Coin(new OutPoint(issuanceCoinsTransaction, 3), issuanceCoinsTransaction.Outputs[3]);

                var goldId = goldIssuanceCoin.AssetId;
                var silverId = silverIssuanceCoin.AssetId;

                chainBuilder.Emit(issuanceCoinsTransaction);
                var b = chainBuilder.SubmitBlock();
                chainBuilder.SyncIndexer();

                var balance = tester.Client.GetOrderedBalance(nico).ToArray();
                var entry = balance[0];
                Assert.NotNull(entry.ColoredTransaction);
                Assert.Equal(Money.Parse("1.0"), entry.Amount);

                txBuilder = new TransactionBuilder();
                var tx = txBuilder
                    .AddKeys(goldGuy)
                    .AddCoins(goldIssuanceCoin)
                    .IssueAsset(nico.GetAddress(), new Asset(goldId, 30))
                    .SetChange(goldGuy.PrivateKey.PubKey)
                    .BuildTransaction(true);

                chainBuilder.Emit(tx);
                b = chainBuilder.SubmitBlock();
                chainBuilder.SyncIndexer();

                var ctx = new IndexerColoredTransactionRepository(tester.Indexer.Configuration);

                balance = tester.Client.GetOrderedBalance(nico.GetAddress()).ToArray();
                var coloredEntry = balance[0];
                Assert.Equal(Money.Parse("0.0"), coloredEntry.Amount);
                Assert.Equal(30, coloredEntry.GetAssetAmount(goldId));

                var coloredCoins = ColoredCoin.Find(tx, ctx).ToArray();
                var nicoGold = coloredCoins[0];

                txBuilder = new TransactionBuilder(1);
                //GoldGuy sends 20 gold to alice against 0.6 BTC. Nico sends 10 gold to alice + 0.02 BTC.
                tx = txBuilder
                    .AddKeys(goldGuy)
                    .AddCoins(goldIssuanceCoin)
                    .IssueAsset(alice.GetAddress(), new Asset(goldId, 20))
                    .SetChange(goldGuy.PrivateKey.PubKey)
                    .Then()
                    .AddKeys(nico.PrivateKey)
                    .AddCoins(nicoCoin)
                    .AddCoins(nicoGold)
                    .SendAsset(alice.GetAddress(), new Asset(goldId, 10))
                    .Send(alice.GetAddress(), Money.Parse("0.02"))
                    .SetChange(nico.GetAddress())
                    .Then()
                    .AddKeys(alice)
                    .AddCoins(aliceCoin)
                    .Send(goldGuy.GetAddress(), Money.Parse("0.6"))
                    .SetChange(alice.GetAddress())
                    .Shuffle()
                    .BuildTransaction(true);

                chainBuilder.Emit(tx);
                b = chainBuilder.SubmitBlock();
                chainBuilder.SyncIndexer();

                //Nico, should have lost 0.02 BTC and 10 gold
                balance = tester.Client.GetOrderedBalance(nico.GetAddress()).ToArray();
                balance = tester.Client.GetOrderedBalance(nico.GetAddress()).ToArray();
                coloredEntry = balance[0];
                Assert.Equal(Money.Parse("-0.02") - txBuilder.ColoredDust, coloredEntry.Amount);
                Assert.Equal(-10, coloredEntry.GetAssetAmount(goldId));

                //Alice, should have lost 0.58 BTC, but win 10 + 20 gold (one is a transfer, the other issuance)
                balance = tester.Client.GetOrderedBalance(alice.GetAddress()).ToArray();
                coloredEntry = balance[0];
                Assert.Equal(Money.Parse("-0.58"), coloredEntry.Amount);
                Assert.Equal(30, coloredEntry.GetAssetAmount(goldId));
            }
        }

        private Block PushStore(BlockStore store, Transaction tx, Block prev = null)
        {
            if (prev == null)
                prev = Network.Main.GetGenesis();
            var b = new Block()
               {
                   Header =
                   {
                       Nonce = RandomUtils.GetUInt32(),
                       HashPrevBlock = prev.GetHash()
                   },
                   Transactions =
					{
						tx
					}
               };
            store.Append(b);
            return b;
        }
        [Fact]
        public void CanImportMainChain()
        {
            using (var tester = CreateTester())
            {
                var node = tester.CreateLocalNode();
                var chain = new ConcurrentChain(tester.Client.Configuration.Network);

                node.ChainBuilder.Generate();
                var fork = node.ChainBuilder.Generate();
                var firstTip = node.ChainBuilder.Generate();
                tester.Indexer.IndexNodeMainChain();

                var result = tester.Client.GetChainChangesUntilFork(chain.Tip, true).ToList();
                Assert.Equal(result[0].BlockId, firstTip.GetHash());
                Assert.Equal(result.Last().BlockId, chain.Tip.HashBlock);
                Assert.Equal(result.Last().Height, chain.Tip.Height);
                Assert.Equal(result.Count, 4);

                result = tester.Client.GetChainChangesUntilFork(chain.Tip, false).ToList();
                Assert.Equal(result[0].BlockId, firstTip.GetHash());
                Assert.NotEqual(result.Last().BlockId, chain.Tip.HashBlock);
                Assert.Equal(result.Count, 3);

                Assert.Equal(firstTip.GetHash(), tester.Client.GetBestBlock().BlockId);

                result.UpdateChain(chain);

                Assert.Equal(firstTip.GetHash(), chain.Tip.HashBlock);

                node.ChainBuilder.Chain.SetTip(fork.Header);
                node.ChainBuilder.Generate();
                node.ChainBuilder.Generate();
                var secondTip = node.ChainBuilder.Generate();

                tester.Indexer.IndexNodeMainChain();
                Assert.Equal(secondTip.GetHash(), tester.Client.GetBestBlock().BlockId);

                result = tester.Client.GetChainChangesUntilFork(chain.Tip, false).ToList();
                result.UpdateChain(chain);
                Assert.Equal(secondTip.GetHash(), chain.Tip.HashBlock);

                var ultimateTip = node.ChainBuilder.Generate(100);
                tester.Indexer.IndexNodeMainChain();
                result = tester.Client.GetChainChangesUntilFork(chain.Tip, false).ToList();

                Assert.Equal(ultimateTip.Header.GetHash(), result[0].BlockId);
                Assert.Equal(tester.Client.GetBestBlock().BlockId, result[0].BlockId);
                result.UpdateChain(chain);
                Assert.Equal(ultimateTip.Header.GetHash(), chain.Tip.HashBlock);

                ConcurrentChain chain2 = new ConcurrentChain();
                var changes = tester.Client.GetChainChangesUntilFork(chain2.Tip, false);
                changes.UpdateChain(chain2);
                Assert.True(chain2.Tip.Height == chain.Tip.Height);
            }
        }

        //[Fact]
        //public void CanGetMultipleEntries()
        //{
        //	var client = new IndexerClient(new IndexerConfiguration()
        //	{
        //		Network = Network.Main,

        //	});

        //	Stopwatch watch = new Stopwatch();
        //	watch.Start();
        //	for(int i = 0 ; i < 10 ; i++)
        //	{
        //		var r = client.GetAllEntries(JsonConvert.DeserializeObject<string[]>(File.ReadAllText("C:/Addresses.txt")).Select(n => new BitcoinScriptAddress(n, Network.Main)).ToArray());
        //	}
        //	watch.Stop();
        //}


        [Fact]
        public void CanGeneratePartitionKey()
        {
            HashSet<string> results = new HashSet<string>();
            while (results.Count != 4096)
            {
                results.Add(Helper.GetPartitionKey(12, RandomUtils.GetBytes(3), 0, 3));
            }
        }

        [Fact]
        public void DoNotCrashOnEmptyScript()
        {
            var tx = new Transaction("01000000014cee27ba570d2cca50bb9b3f7374c7eb24ec16ffec0a077c84c1cc23b0161804010000008b48304502200f1100f78596c8d46fb2f39c570ce6945956a3dd33c48fbdbe53af1c383182ed022100a85b528ea21ee7f39b2ec1568ac19f26f4dd4fb9d3dbf70587986de3c2c90fa801410426e4d0890ad5272b2b9a10ca3f518f7e025932caa62f13467e444df89ed25f24f4fc5075cad32f468c8f7f913e30057449d65623726e7102f5eaa326d486ebf7ffffffff020010000000000000006020e908000000001976a914947236437233a71cb033a53932008dbfe346388e88ac00000000");
            OrderedBalanceChange.ExtractScriptBalances(null, tx, null, null, 0);
        }


        TransactionSignature sig = new TransactionSignature(Encoders.Hex.DecodeData("304602210095050cbad0bc3bad2436a651810e83f21afb1cdf75d74a13049114958942067d02210099b591d52665597fd88c4a205fe3ef82715e5a125e0f2ae736bf64dc634fba9f01"));



        [Fact]
        public void CanMergeBalance()
        {
            using (var tester = CreateTester())
            {
                var bob = new Key();
                var alice1 = new Key();
                var alice2 = new Key();
                var satoshi = new Key();

                var chainBuilder = tester.CreateChainBuilder();
                chainBuilder.EmitMoney(bob, "50.0");
                var tx = chainBuilder.EmitMoney(alice1, "10.0");
                chainBuilder.SubmitBlock();
                chainBuilder.SyncIndexer();

                //Can merge address balance into wallet
                tester.Client.MergeIntoWallet("Alice", alice1);

                var aliceBalance = tester.Client.GetOrderedBalance("Alice").ToArray();
                Assert.True(aliceBalance.Length == 1);
                Assert.True(aliceBalance[0].Amount == Money.Parse("10.0"));
                Assert.True(aliceBalance[0].IsCoinbase);
                Assert.True(aliceBalance[0].ScriptPubKey == alice1.ScriptPubKey);
                Assert.True(!aliceBalance[0].HasOpReturn);
                ////

                //Merging duplicate order balance should not change anything
                tester.Client.AddWalletRule("Alice", new ScriptRule(alice1));

                chainBuilder.EmitMoney(alice1, "9.0");
                chainBuilder.SubmitBlock();
                chainBuilder.SyncIndexer();

                aliceBalance = tester.Client.GetOrderedBalance("Alice").ToArray();
                Assert.True(aliceBalance.Length == 2);
                Assert.True(aliceBalance[0].Amount == Money.Parse("9.0"));
                Assert.True(aliceBalance[0].ScriptPubKey == alice1.ScriptPubKey);

                tester.Client.MergeIntoWallet("Alice", alice1);
                aliceBalance = tester.Client.GetOrderedBalance("Alice").ToArray();
                Assert.True(aliceBalance.Length == 2);
                Assert.True(aliceBalance[0].Amount == Money.Parse("9.0"));
                Assert.True(aliceBalance[0].ScriptPubKey == alice1.ScriptPubKey);
                Assert.True(aliceBalance[1].Amount == Money.Parse("10.0"));
                Assert.True(aliceBalance[1].ScriptPubKey == alice1.ScriptPubKey);
                ////

                //Merge alice2 into Alice with a tx involving alice1
                tx
                   = new TransactionBuilder()
                       .AddKeys(alice1)
                       .AddCoins(new Coin(tx.GetHash(), 0, tx.Outputs[0].Value, tx.Outputs[0].ScriptPubKey)) //Alice1 10
                       .Send(alice2, "2.0")
                       .Send(alice1, "3.9")
                       .Send(bob, "2.1")
                       .Send(alice1, "0.1")
                       .SendFees("1.9")
                       .BuildTransaction(true);

                chainBuilder.Emit(tx);
                chainBuilder.SubmitBlock();
                chainBuilder.SyncIndexer();

                aliceBalance = tester.Client.GetOrderedBalance("Alice").ToArray();
                Assert.True(aliceBalance.Length == 3);
                Assert.True(aliceBalance[0].Amount ==
                    -Money.Parse("10.0")
                    + Money.Parse("3.9")
                    + Money.Parse("0.1"));
                Assert.True(aliceBalance[0].ScriptPubKey == alice1.ScriptPubKey);

                tester.Client.MergeIntoWallet("Alice", alice2);
                aliceBalance = tester.Client.GetOrderedBalance("Alice").ToArray();
                Assert.True(aliceBalance.Length == 3);
                Assert.True(aliceBalance[0].Amount ==
                    -Money.Parse("10.0")
                    + Money.Parse("3.9")
                    + Money.Parse("0.1")
                    + Money.Parse("2.0"));
                Assert.True(aliceBalance[0].ScriptPubKey == alice1.ScriptPubKey);

                var newtx = new Transaction()
                {
                    Inputs =
                    {
                        new TxIn(new OutPoint(tx,0)), //alice2 2
                        new TxIn(new OutPoint(tx,1)), //alice1 3.9
                        new TxIn(new OutPoint(tx,2)), //bob 2.1
                        new TxIn(new OutPoint(tx,3)), //alice1 0.1
                    }
                };

                tx = new TransactionBuilder()
                        .ContinueToBuild(newtx)
                        .AddKeys(alice1, alice2)
                        .AddCoins(new Coin(tx.GetHash(), 0, tx.Outputs[0].Value, tx.Outputs[0].ScriptPubKey))
                        .AddCoins(new Coin(tx.GetHash(), 1, tx.Outputs[1].Value, tx.Outputs[1].ScriptPubKey))
                        .AddCoins(new Coin(tx.GetHash(), 3, tx.Outputs[3].Value, tx.Outputs[3].ScriptPubKey))
                        .Then()
                        .AddKeys(bob)
                        .AddCoins(new Coin(tx.GetHash(), 2, tx.Outputs[2].Value, tx.Outputs[2].ScriptPubKey))
                        .Send(alice1, "0.10")
                        .Send(alice2, "0.22")
                        .Send(bob, "1.0")
                        .Send(alice2, "0.23")
                        .SetChange(satoshi)
                        .BuildTransaction(true);

                chainBuilder.Emit(tx);
                chainBuilder.SubmitBlock();
                chainBuilder.SyncIndexer();

                aliceBalance = tester.Client.GetOrderedBalance("Alice").ToArray();
                Assert.True(aliceBalance.Length == 4);
                Assert.True(aliceBalance[0].Amount ==
                    -Money.Parse("3.9")
                    - Money.Parse("0.1")
                    + Money.Parse("0.10")
                    );
                Assert.True(aliceBalance[0].ScriptPubKey == alice1.ScriptPubKey);

                tester.Client.MergeIntoWallet("Alice", alice2, new ScriptRule()
                {
                    ScriptPubKey = alice2.ScriptPubKey,
                    CustomData = "hello"
                });
                aliceBalance = tester.Client.GetOrderedBalance("Alice").ToArray();
                Assert.True(aliceBalance.Length == 4);
                Assert.True(aliceBalance[0].Amount ==
                    -Money.Parse("3.9")
                    - Money.Parse("0.1")
                    + Money.Parse("0.10")
                    - Money.Parse("2.0")
                    + Money.Parse("0.22")
                    + Money.Parse("0.23")
                    );
                Assert.True(aliceBalance[0].ScriptPubKey == alice1.ScriptPubKey);
                Assert.True(aliceBalance[0].MatchedRules.Any(m => m.Rule.CustomData == "hello"));
                ////
            }
        }

        [Fact]
        public void CanGetWalletOrderedBalances()
        {
            using (var tester = CreateTester())
            {
                var bob = new Key();
                var alice1 = new Key();
                var alice2 = new Key();
                var satoshi = new Key();

                var expectedRule = tester.Client.AddWalletRule("Alice", new ScriptRule(alice1)
                {
                    CustomData = "hello"
                });
                Assert.True(expectedRule.Rule.ToString().Contains("hello"));
                var rules = tester.Client.GetWalletRules("Alice");
                Assert.Equal(1, rules.Length);
                Assert.Equal(expectedRule.WalletId, rules[0].WalletId);
                Assert.Equal(expectedRule.Rule.ToString(), rules[0].Rule.ToString());
                var aliceR1 = expectedRule.Rule;

                var chainBuilder = tester.CreateChainBuilder();
                chainBuilder.EmitMoney(bob, "50.0");
                var tx = chainBuilder.EmitMoney(alice1, "10.0");
                chainBuilder.SubmitBlock();
                chainBuilder.SyncIndexer();

                var aliceBalance = tester.Client.GetOrderedBalance("Alice").ToArray();
                Assert.True(aliceBalance.Length == 1);
                Assert.Equal("Alice", aliceBalance[0].BalanceId.GetWalletId());
                Assert.True(aliceBalance[0].Amount == Money.Parse("10.0"));
                Assert.True(aliceBalance[0].IsCoinbase);
                Assert.True(aliceBalance[0].ScriptPubKey == alice1.ScriptPubKey);
                Assert.True(!aliceBalance[0].HasOpReturn);
                Assert.Equal(
                    aliceR1.ToString()
                   , aliceBalance[0].GetMatchedRules(0, MatchLocation.Output).First().ToString());

                var aliceR2 = tester.Client.AddWalletRule("Alice", new ScriptRule(alice2)).Rule;
                rules = tester.Client.GetWalletRules("Alice");
                Assert.Equal(2, rules.Length);

                //Adding two time same rule should be idempotent
                tester.Client.AddWalletRule("Alice", new ScriptRule(alice2));
                Assert.Equal(2, rules.Length);
                /////////////////////////////////////////////


                tx
                    = new TransactionBuilder()
                        .AddKeys(alice1)
                        .AddCoins(new Coin(tx.GetHash(), 0, tx.Outputs[0].Value, tx.Outputs[0].ScriptPubKey))
                        .Send(alice2, "2.0")
                        .Send(alice1, "3.9")
                        .Send(bob, "2.1")
                        .Send(alice1, "0.1")
                        .SendFees("1.9")
                        .BuildTransaction(true);

                chainBuilder.Emit(tx);
                chainBuilder.SubmitBlock();
                chainBuilder.SyncIndexer();


                aliceBalance = tester.Client.GetOrderedBalance("Alice").ToArray();
                Assert.True(aliceBalance[0].Amount == Money.Parse("-4.0"));

                Assert.Equal(
                   aliceR1.ToString()
                  , aliceBalance[0].GetMatchedRules(aliceBalance[0].SpentCoins[0]).First().ToString());

                Assert.Equal(
                   aliceR2.ToString()
                  , aliceBalance[0].GetMatchedRules(0, MatchLocation.Output).First().ToString());

                Assert.Equal(
                   aliceR1.ToString()
                  , aliceBalance[0].GetMatchedRules(1, MatchLocation.Output).First().ToString());

                Assert.Equal(
                aliceR1.ToString()
               , aliceBalance[0].GetMatchedRules(3, MatchLocation.Output).First().ToString());

                Assert.True(aliceBalance[0].GetMatchedRules(2, MatchLocation.Output).Count() == 0);

                var prevTx = tx;
                var newtx = new Transaction()
                {
                    Inputs =
                    {
                        new TxIn(new OutPoint(tx,0)), //alice2 2
                        new TxIn(new OutPoint(tx,1)), //alice1 3.9
                        new TxIn(new OutPoint(tx,2)), //bob 2.1
                        new TxIn(new OutPoint(tx,3)), //alice1 0.1
                    }
                };

                tx = new TransactionBuilder()
                        .ContinueToBuild(newtx)
                        .AddKeys(alice1, alice2)
                        .AddCoins(new Coin(prevTx.GetHash(), 0, prevTx.Outputs[0].Value, prevTx.Outputs[0].ScriptPubKey))
                        .AddCoins(new Coin(prevTx.GetHash(), 1, prevTx.Outputs[1].Value, prevTx.Outputs[1].ScriptPubKey))
                        .AddCoins(new Coin(prevTx.GetHash(), 3, prevTx.Outputs[3].Value, prevTx.Outputs[3].ScriptPubKey))
                        .Then()
                        .AddKeys(bob)
                        .AddCoins(new Coin(prevTx.GetHash(), 2, prevTx.Outputs[2].Value, prevTx.Outputs[2].ScriptPubKey))
                        .Send(alice1, "0.10")
                        .Send(alice2, "0.22")
                        .Send(bob, "1.0")
                        .Send(alice2, "0.23")
                        .SetChange(satoshi)
                        .BuildTransaction(true);

                chainBuilder.Emit(tx);
                var b3 = chainBuilder.SubmitBlock();
                chainBuilder.SyncIndexer();


                aliceBalance = tester.Client.GetOrderedBalance("Alice").ToArray();
                var entry = aliceBalance[0];

                Assert.Equal(entry.GetMatchedRules(new OutPoint(prevTx, 0)).First().ToString(), aliceR2.ToString());
                Assert.Equal(entry.GetMatchedRules(new OutPoint(prevTx, 1)).First().ToString(), aliceR1.ToString());
                Assert.Null(entry.GetMatchedRules(new OutPoint(prevTx, 2)).FirstOrDefault());
                Assert.Equal(entry.GetMatchedRules(new OutPoint(prevTx, 3)).First().ToString(), aliceR1.ToString());

                var receivedOutpoints = tx.Outputs.Select((o, i) => new OutPoint(tx.GetHash(), i)).ToArray();
                Assert.Equal(entry.GetMatchedRules(new OutPoint(tx, 1)).First().ToString(), aliceR1.ToString());
                Assert.Equal(entry.GetMatchedRules(new OutPoint(tx, 2)).First().ToString(), aliceR2.ToString());
                Assert.Null(entry.GetMatchedRules(new OutPoint(tx, 3)).FirstOrDefault());
                Assert.Equal(entry.GetMatchedRules(new OutPoint(tx, 4)).First().ToString(), aliceR2.ToString());
                ////

                //Send money to P2SH address, should receive script coins

                tester.Client.AddWalletRule("Alice", new ScriptRule(alice1.PubKey, true));
                tester.Client.AddWalletRule("Alice", new ScriptRule(alice2.PubKey.ScriptPubKey.Hash, false));

                tx = new TransactionBuilder()
                        .ContinueToBuild(newtx)
                        .AddKeys(alice1, alice2)
                        .AddCoins(new Coin(prevTx.GetHash(), 0, prevTx.Outputs[0].Value, prevTx.Outputs[0].ScriptPubKey))
                        .AddCoins(new Coin(prevTx.GetHash(), 1, prevTx.Outputs[1].Value, prevTx.Outputs[1].ScriptPubKey))
                        .AddCoins(new Coin(prevTx.GetHash(), 3, prevTx.Outputs[3].Value, prevTx.Outputs[3].ScriptPubKey))
                        .Then()
                        .AddKeys(bob)
                        .AddCoins(new Coin(prevTx.GetHash(), 2, prevTx.Outputs[2].Value, prevTx.Outputs[2].ScriptPubKey))
                        .Send(alice1.PubKey.ScriptPubKey.Hash, "0.10")
                        .Send(alice2.PubKey.ScriptPubKey.Hash, "0.22")
                        .Send(bob, "1.0")
                        .Send(alice2.PubKey.ScriptPubKey.Hash, "0.23")
                        .SetChange(satoshi)
                        .BuildTransaction(true);


                chainBuilder.Emit(tx);
                chainBuilder.SubmitBlock();
                chainBuilder.SyncIndexer();

                aliceBalance = tester.Client.GetOrderedBalance("Alice").ToArray();
                Assert.True(aliceBalance[0].ReceivedCoins[0] is ScriptCoin);
                Assert.True(aliceBalance[0].ReceivedCoins[0].TxOut.ScriptPubKey == alice1.PubKey.ScriptPubKey.Hash.ScriptPubKey);
                Assert.True(((ScriptCoin)(aliceBalance[0].ReceivedCoins[0])).Redeem == alice1.PubKey.ScriptPubKey);
                Assert.False(aliceBalance[0].ReceivedCoins[1] is ScriptCoin);
                Assert.False(aliceBalance[0].ReceivedCoins[2] is ScriptCoin);

                tx = new TransactionBuilder()
                        .AddKeys(alice1, alice2)
                        .AddCoins(aliceBalance[0].ReceivedCoins[0])
                        .Send(satoshi, "0.0001")
                        .SetChange(alice1)
                        .BuildTransaction(true);
                chainBuilder.Emit(tx);
                chainBuilder.SubmitBlock();
                chainBuilder.SyncIndexer();

                var aliceBalance2 = tester.Client.GetOrderedBalance("Alice").ToArray();
                Assert.True(((ScriptCoin)(aliceBalance2[0].SpentCoins[0])).Redeem == alice1.PubKey.ScriptPubKey);
                /////
            }

        }

        [Fact]
        public void CanQueryBalanceRange()
        {
            using (var tester = CreateTester())
            {
                Key bob = new BitcoinSecret("L4JinGSmHxKJJrjbeFx3zxf9Vr3VD6jmq5wXpDm6ywUewcWoXEAy").PrivateKey;
                var chainBuilder = tester.CreateChainBuilder();
                chainBuilder.NoRandom = true;

                Dictionary<string, Transaction> txs = new Dictionary<string, Transaction>();
                txs.Add("tx1", chainBuilder.EmitMoney(bob, "1.0"));
                chainBuilder.SubmitBlock();

                txs.Add("tx21", chainBuilder.EmitMoney(bob, "2.1"));
                txs.Add("tx22", chainBuilder.EmitMoney(bob, "2.2"));
                chainBuilder.SubmitBlock();

                txs.Add("tx31", chainBuilder.EmitMoney(bob, "3.1"));
                txs.Add("tx32", chainBuilder.EmitMoney(bob, "3.2"));
                txs.Add("tx33", chainBuilder.EmitMoney(bob, "3.3"));
                chainBuilder.SubmitBlock();

                txs.Add("tx41", chainBuilder.EmitMoney(bob, "4.2"));
                txs.Add("tx42", chainBuilder.EmitMoney(bob, "4.3"));
                txs.Add("tx43", chainBuilder.EmitMoney(bob, "4.1"));
                chainBuilder.SubmitBlock();

                txs.Add("utx51", chainBuilder.EmitMoney(bob, "5.1", isCoinbase: false, indexBalance: true));
                Thread.Sleep(1000);
                txs.Add("utx52", chainBuilder.EmitMoney(bob, "5.2", isCoinbase: false, indexBalance: true));
                Thread.Sleep(1000);
                txs.Add("utx53", chainBuilder.EmitMoney(bob, "5.3", isCoinbase: false, indexBalance: true));

                chainBuilder.SyncIndexer();


                var tests = new String[][]
                {
                     new string[]{"2in", "4in", "tx43,tx42,tx41,tx33,tx32,tx31,tx22,tx21"},
                     new string[]{"4in", "2in", "tx43,tx42,tx41,tx33,tx32,tx31,tx22,tx21"}, //Does not care about order
                     new string[]{"2ex", "4in", "tx43,tx42,tx41,tx33,tx32,tx31"},
                     new string[]{"2in", "4ex", "tx33,tx32,tx31,tx22,tx21"},
                     new string[]{"2ex", "4ex", "tx33,tx32,tx31"},
                     new string[]{"{utx51}in", "{utx53}in", "utx53,utx52,utx51"},
                     new string[]{"{utx51}in", "{utx53}ex", "utx52,utx51"},
                     new string[]{"{utx51}ex", "{utx53}in", "utx53,utx52"},
                     new string[]{"{utx52}ex", "3in", "utx51,tx43,tx42,tx41,tx33,tx32,tx31"}
                };

                var all = tester.Client.GetOrderedBalance(bob).ToArray();
                Assert.Equal(all.Length, txs.Count);

                foreach (var test in tests)
                {
                    var data = test;
                    BalanceQuery query = new BalanceQuery();
                    query.From = Parse(data[0], all, txs);
                    query.FromIncluded = ParseIncl(data[0]);
                    query.To = Parse(data[1], all, txs);
                    query.ToIncluded = ParseIncl(data[1]);

                    var result = tester.Client.GetOrderedBalance(bob, query).ToArray();
                    var expected = data[2].Split(',').ToArray();

                    var expectedResult = String.Join(",", expected);
                    var actualResult = String.Join(",", result.Select(o => GetName(txs, o)));
                    Assert.Equal(expectedResult, actualResult);
                }
            }
        }

        private string GetName(Dictionary<string, Transaction> txs, OrderedBalanceChange change)
        {
            var name = txs.Single(t => t.Value.GetHash() == change.TransactionId).Key;
            var unconf1 = name.StartsWith("u");
            var unconf2 = change.BlockId == null;
            if (unconf1 != unconf2)
                Assert.False(true, "A confirmed or unconfirmed transaction should not have been returned");
            return name;
        }

        private bool ParseIncl(string included)
        {
            return included.EndsWith("in");
        }

        private BalanceLocator Parse(string loc, OrderedBalanceChange[] changes, Dictionary<string, Transaction> txs)
        {
            if (loc.Contains("{"))
            {
                var res = Regex.Match(loc, "{(.*?)}");
                var tx = txs[res.Groups[1].Value];
                var change = changes.Single(c => c.TransactionId == tx.GetHash());
                return new UnconfirmedBalanceLocator(change.SeenUtc, tx.GetHash());
            }
            else
            {
                return BalanceLocator.Parse(loc.Substring(0, loc.Length - 2));
            }
        }


        [Fact]
        public void CanGetBalanceSheet()
        {
            using (var tester = CreateTester())
            {
                var bob = new Key();
                var alice = new Key();
                var satoshi = new Key();

                var chainBuilder = tester.CreateChainBuilder();
                chainBuilder.EmitMoney(bob, "50.0");
                chainBuilder.EmitMoney(alice, "50.0");
                chainBuilder.SubmitBlock();

                chainBuilder.EmitMoney(bob, "20.0");
                chainBuilder.SubmitBlock();

                chainBuilder.SyncIndexer();

                var sheet = tester.Client.GetOrderedBalance(bob).AsBalanceSheet(chainBuilder.Chain);
                Assert.True(sheet.Confirmed.Count == 2);
                Assert.True(sheet.Unconfirmed.Count == 0);
                Assert.True(sheet.Prunable.Count == 0);
                Assert.True(sheet.All.Count == 2);
                Assert.True(sheet.All[0].Amount == Money.Parse("20.0"));

                var tx = chainBuilder.EmitMoney(bob, "10.0", false);
                tester.Indexer.Index(new TransactionEntry.Entity(null, tx, null));
                tester.Indexer.IndexOrderedBalance(tx);

                sheet = tester.Client.GetOrderedBalance(bob).AsBalanceSheet(chainBuilder.Chain);
                Assert.True(sheet.Confirmed.Count == 2);
                Assert.True(sheet.Unconfirmed.Count == 1);
                Assert.True(sheet.Prunable.Count == 0);
                Assert.True(sheet.All.Count == 3);
                Assert.True(sheet.All[0].Amount == Money.Parse("10.0"));

                chainBuilder.SubmitBlock();
                chainBuilder.SyncIndexer();

                sheet = tester.Client.GetOrderedBalance(bob).AsBalanceSheet(chainBuilder.Chain);
                Assert.True(sheet.Confirmed.Count == 3);
                Assert.True(sheet.Unconfirmed.Count == 0);
                Assert.True(sheet.Prunable.Count == 1);
                Assert.True(sheet.All.Count == 3);
                Assert.True(sheet.All[0].Amount == Money.Parse("10.0"));
                Assert.True(sheet.All[0].BlockId != null);

                var latest = sheet.All[0];
                sheet = tester.Client.GetOrderedBalance(bob, new BalanceQuery()
                {
                    From = sheet.All[0].CreateBalanceLocator(),
                    FromIncluded = false
                }).AsBalanceSheet(chainBuilder.Chain);

                Assert.True(sheet.Confirmed.Count == 2);

                sheet = tester.Client.GetOrderedBalance(bob, new BalanceQuery()
                {
                    From = latest.CreateBalanceLocator(),
                    FromIncluded = true,
                    To = sheet.All[1].CreateBalanceLocator(),
                    ToIncluded = true
                }).AsBalanceSheet(chainBuilder.Chain);

                Assert.True(sheet.Confirmed.Count == 3);
                Assert.True(sheet.Prunable.Count == 0); //No mempool balance

                sheet = tester.Client.GetOrderedBalance(bob, new BalanceQuery()
                {
                    To = sheet.All[2].CreateBalanceLocator(),
                    ToIncluded = true
                }).AsBalanceSheet(chainBuilder.Chain);

                Assert.True(sheet.Confirmed.Count == 3);
                Assert.True(sheet.Prunable.Count == 1);

                tester.Client.PruneBalances(sheet.Prunable);

                sheet = tester.Client.GetOrderedBalance(bob).AsBalanceSheet(chainBuilder.Chain);
                Assert.True(sheet.Confirmed.Count == 3);
                Assert.True(sheet.Unconfirmed.Count == 0);
                Assert.True(sheet.Prunable.Count == 0);
                Assert.True(sheet.All.Count == 3);
                Assert.True(sheet.All[0].Amount == Money.Parse("10.0"));
                Assert.True(sheet.All[0].BlockId != null);

                sheet.All[0].CustomData = "test";
                tester.Indexer.Index(new OrderedBalanceChange[] { sheet.All[0] });

                sheet = tester.Client.GetOrderedBalance(bob).AsBalanceSheet(chainBuilder.Chain);
                Assert.True(sheet.All[0].CustomData == "test");
            }
        }

        [Fact]
        public void CanFastEncode()
        {
            byte[] bytes = new byte[] { 0xFF, 1, 2, 3, 0 };
            var str = FastEncoder.Instance.EncodeData(bytes);
            byte[] actual = FastEncoder.Instance.DecodeData(str);
            Assert.True(bytes.SequenceEqual(actual));

            for (int i = 0 ; i < 1000 ; i++)
            {
                bytes = RandomUtils.GetBytes(100);
                str = FastEncoder.Instance.EncodeData(bytes);
                actual = FastEncoder.Instance.DecodeData(str);
                Assert.False(str.Contains('-'));
                Assert.True(bytes.SequenceEqual(actual));
            }
        }

        [Fact]
        public void Play()
        {

        }

        [Fact]
        public void CanIndexLongScript()
        {
            using (var tester = CreateTester())
            {
                var tx = new Transaction("010000000127d57276f1026a95b4af3b03b6aba859a001861682342af19825e8a2408ae008010000008c493046022100cd92b992d4bde3b44471677081c5ece6735d6936480ff74659ac1824d8a1958e022100b08839f167532aea10acecc9d5f7044ddd9793ef2989d090127a6e626dc7c9ce014104cac6999d6c3feaba7cdd6c62bce174339190435cffd15af7cb70c33b82027deba06e6d5441eb401c0f8f92d4ffe6038d283d2b2dd59c4384b66b7b8f038a7cf5ffffffff0200093d0000000000434104636d69f81d685f6f58054e17ac34d16db869bba8b3562aabc38c35b065158d360f087ef7bd8b0bcbd1be9a846a8ed339bf0131cdb354074244b0a9736beeb2b9ac40420f0000000000fdba0f76a9144838a081d73cf134e8ff9cfd4015406c73beceb388acacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacac00000000");
                tester.Indexer.IndexOrderedBalance(tx);
                var result = tester.Client.GetOrderedBalance(tx.Outputs[1].ScriptPubKey).ToArray()[0];
                Assert.Equal(result.ScriptPubKey, tx.Outputs[1].ScriptPubKey);
            }
        }

        [Fact]
        public void CanGetOrderedBalances()
        {
            using (var tester = CreateTester())
            {
                var bob = new Key();
                var alice = new Key();
                var satoshi = new Key();

                var chainBuilder = tester.CreateChainBuilder();
                chainBuilder.EmitMoney(bob, "50.0");
                chainBuilder.EmitMoney(alice, "50.0");
                chainBuilder.SubmitBlock();

                chainBuilder.EmitMoney(bob, "20.0");
                chainBuilder.SubmitBlock();

                chainBuilder.SyncIndexer();

                var bobBalance = tester.Client.GetOrderedBalance(bob).ToArray();
                Assert.True(bobBalance.Length == 2);
                Assert.True(bobBalance[0].Amount == Money.Parse("20.0"));
                Assert.True(bobBalance[0].IsCoinbase);
                Assert.True(!bobBalance[0].HasOpReturn);
                Assert.True(bobBalance[1].Amount == Money.Parse("50.0"));

                var aliceBalance = tester.Client.GetOrderedBalance(alice).ToArray();
                var tx = new TransactionBuilder()
                    .AddCoins(bobBalance[0].ReceivedCoins)
                    .AddKeys(bob)
                    .Send(alice, "5.0")
                    .SetChange(bob)
                    .Then()
                    .AddCoins(aliceBalance[0].ReceivedCoins)
                    .AddKeys(alice)
                    .Send(satoshi, "1.0")
                    .SendFees("0.05")
                    .SetChange(alice)
                    .BuildTransaction(true);
                tx.AddOutput(new TxOut(Money.Zero, TxNullDataTemplate.Instance.GenerateScriptPubKey(RandomUtils.GetBytes(3)))); //Add OP_RETURN
                chainBuilder.Emit(tx, false);
                var block = chainBuilder.SubmitBlock();
                chainBuilder.SyncIndexer();

                bobBalance = tester.Client.GetOrderedBalance(bob).ToArray();
                Assert.True(bobBalance[0].Amount == -Money.Parse("5.0"));

                for (int i = 0 ; i < 2 ; i++)
                {

                    aliceBalance = tester.Client.GetOrderedBalance(alice).ToArray();
                    Assert.True(aliceBalance[0].Amount == -Money.Parse("1.0") - Money.Parse("0.05") + Money.Parse("5.0"));

                    Assert.True(aliceBalance[0].SpentIndices.Count == 1);
                    Assert.True(aliceBalance[0].SpentIndices[0] == 1);
                    Assert.True(aliceBalance[0].SpentOutpoints[0] == tx.Inputs[1].PrevOut);
                    Assert.True(aliceBalance[0].SpentCoins[0].Outpoint == aliceBalance[1].ReceivedCoins[0].Outpoint);
                    Assert.True(aliceBalance[0].TransactionId == tx.GetHash());
                    Assert.True(aliceBalance[0].Height == 3);
                    Assert.True(aliceBalance[0].BlockId == block.GetHash());
                    Assert.True(!aliceBalance[0].IsCoinbase);
                    Assert.True(aliceBalance[0].HasOpReturn);
                    Assert.True(aliceBalance[0].ReceivedCoins[0].Outpoint == new OutPoint(tx.GetHash(), 1)); //Bob coin
                    Assert.True(aliceBalance[0].ReceivedCoins[1].Outpoint == new OutPoint(tx.GetHash(), 2)); //Change
                }

                var satoshiBalance = tester.Client.GetOrderedBalance(satoshi).ToArray();
                Assert.True(satoshiBalance[0].Amount == Money.Parse("1.0"));

                tx = new TransactionBuilder()
                        .AddCoins(satoshiBalance[0].ReceivedCoins)
                        .AddKeys(satoshi)
                        .Send(alice, "0.2")
                        .SetChange(satoshi)
                        .BuildTransaction(true);

                tester.Indexer.Index(new TransactionEntry.Entity(null, tx, null));
                tester.Indexer.IndexOrderedBalance(tx);

                tx = new TransactionBuilder()
                       .AddCoins(satoshiBalance[0].ReceivedCoins)
                       .AddKeys(satoshi)
                       .Send(alice, "0.3")
                       .SetChange(satoshi)
                       .BuildTransaction(true);

                tester.Indexer.Index(new TransactionEntry.Entity(null, tx, null));
                tester.Indexer.IndexOrderedBalance(tx);

                satoshiBalance = tester.Client.GetOrderedBalance(satoshi).ToArray();
                Assert.True(satoshiBalance[0].Amount == -Money.Parse("0.3"));
                Assert.True(satoshiBalance[1].Amount == -Money.Parse("0.2"));

                tx = new TransactionBuilder()
                       .AddCoins(satoshiBalance[0].ReceivedCoins)
                       .AddKeys(satoshi)
                       .Send(alice, "0.1")
                       .SetChange(satoshi)
                       .BuildTransaction(true);

                Thread.Sleep(1000);
                chainBuilder.Emit(tx);
                chainBuilder.SubmitBlock();
                chainBuilder.SyncIndexer();

                satoshiBalance = tester.Client.GetOrderedBalance(satoshi).ToArray();
                Assert.True(satoshiBalance[0].Amount == -Money.Parse("0.1"));

                tester.Client.CleanUnconfirmedChanges(satoshi, TimeSpan.Zero);

                satoshiBalance = tester.Client.GetOrderedBalance(satoshi).ToArray();
                Assert.True(satoshiBalance.Length == 2);
            }
        }



        [Fact]
        public void CanGetBlock()
        {
            using (var tester = CreateTester("cached"))
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
            using (var tester = CreateTester("cached"))
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


        [Fact]
        public void CanGetColoredTransaction()
        {
            using (var tester = CreateTester())
            {
                var node = tester.CreateLocalNode();
                var ccTester = new ColoredCoinTester("CanColorizeTransferTransaction");
                node.ChainBuilder.Emit(ccTester.Transactions);
                node.ChainBuilder.SubmitBlock();
                tester.Indexer.IndexBlocks();
                tester.Indexer.IndexTransactions();
                var txRepo = new IndexerTransactionRepository(tester.Indexer.Configuration);
                var indexedTx = txRepo.Get(ccTester.TestedTxId);
                Assert.NotNull(indexedTx);
                Assert.Null(txRepo.Get(tester.UnknownTransactionId));

                var ccTxRepo = new IndexerColoredTransactionRepository(tester.Indexer.Configuration);
                var colored = ccTxRepo.Get(ccTester.TestedTxId);
                Assert.Null(colored);

                colored = ColoredTransaction.FetchColors(ccTester.TestedTxId, ccTxRepo);
                Assert.NotNull(colored);

                colored = ccTxRepo.Get(ccTester.TestedTxId);
                Assert.NotNull(colored);
            }
        }


        private IndexerTester CreateTester([CallerMemberName]string folder = null)
        {
            return new IndexerTester(folder);
        }
    }

    class ColoredCoinTester
    {
        public ColoredCoinTester([CallerMemberName]string test = null)
        {
            var testcase = JsonConvert.DeserializeObject<TestCase[]>(File.ReadAllText("Data/openasset-known-tx.json"))
                .First(t => t.test == test);
            NoSqlTransactionRepository repository = new NoSqlTransactionRepository();
            foreach (var tx in testcase.txs)
            {
                var txObj = new Transaction(tx);
                Transactions.Add(txObj);
                repository.Put(txObj.GetHash(), txObj);
            }
            TestedTxId = new uint256(testcase.testedtx);
            Repository = new NoSqlColoredTransactionRepository(repository, new InMemoryNoSqlRepository());
        }


        public IColoredTransactionRepository Repository
        {
            get;
            set;
        }

        public uint256 TestedTxId
        {
            get;
            set;
        }

        public string AutoDownloadMissingTransaction(Action act)
        {
            StringBuilder builder = new StringBuilder();
            while (true)
            {
                try
                {
                    act();
                    break;
                }
                catch (TransactionNotFoundException ex)
                {
                    WebClient client = new WebClient();
                    var result = client.DownloadString("http://btc.blockr.io/api/v1/tx/raw/" + ex.TxId);
                    var json = JObject.Parse(result);
                    var tx = new Transaction(json["data"]["tx"]["hex"].ToString());

                    builder.AppendLine("\"" + json["data"]["tx"]["hex"].ToString() + "\",\r\n");
                    Repository.Transactions.Put(tx.GetHash(), tx);
                }
            }
            return builder.ToString();
        }

        public List<Transaction> Transactions = new List<Transaction>();
    }

    class TestCase
    {
        public string test
        {
            get;
            set;
        }
        public string testedtx
        {
            get;
            set;
        }
        public string[] txs
        {
            get;
            set;
        }
    }
}
