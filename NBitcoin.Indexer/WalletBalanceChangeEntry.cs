using Microsoft.WindowsAzure.Storage.Table;
using NBitcoin.Indexer.Internal;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NBitcoin.Indexer
{
    public class WalletBalanceChangeEntry : BalanceChangeEntry
    {
        public WalletBalanceChangeEntry(params Entity[] entities)
        {
            Init(entities);
            var entity = entities[0];
            _MatchedRulesByOutputIndex = entity.MatchedRulesByOutputIndex;
            foreach (var kv in entity.MatchedRulesByInputOutpointIndex)
            {
                _MatchedRulesByOutpoint.Add(entity.SpentOutpoints[(int)kv.Key], kv.Value);
            }
        }

        public string WalletId
        {
            get
            {
                return BalanceId;
            }
        }

        public new WalletBalanceChangeEntry FetchConfirmedBlock(Chain chain)
        {
            return (WalletBalanceChangeEntry)base.FetchConfirmedBlock(chain);
        }

        public new class Entity : BalanceChangeEntry.Entity
        {
            class UIntPair : IBitcoinSerializable
            {
                uint _N;
                public uint N
                {
                    get
                    {
                        return _N;
                    }
                    set
                    {
                        _N = value;
                    }
                }
                string _Rule;
                public string Rule
                {
                    get
                    {
                        return _Rule;
                    }
                    set
                    {
                        _Rule = value;
                    }
                }

                public void ReadWrite(BitcoinStream stream)
                {
                    stream.ReadWriteAsVarInt(ref _N);
                    if (stream.Serializing)
                    {
                        var bytes = Encoding.UTF8.GetBytes(Rule);
                        stream.ReadWriteAsVarString(ref bytes);
                    }
                    else
                    {
                        var bytes = new byte[0];
                        stream.ReadWriteAsVarString(ref bytes);
                        Rule = Encoding.UTF8.GetString(bytes);
                    }
                }
            }
            public Entity(uint256 txid, string walletId, uint256 blockId)
                : base(txid, walletId, blockId)
            {
            }
            public Entity(DynamicTableEntity entity, IndexerClient client)
                : base(entity)
            {
                var bytes = Helper.GetEntityProperty(entity, "matches");
                BitcoinStream stream = new BitcoinStream(bytes);
                UIntPair[] pairs = new UIntPair[0];
                stream.ReadWrite(ref pairs);
                foreach (var p in pairs)
                {
                    _MatchedRulesByInputOutpointIndex.Add(p.N, client.DeserializeRule(p.Rule));
                }
                stream.ReadWrite(ref pairs);
                foreach (var p in pairs)
                {
                    _MatchedRulesByOutputIndex.Add(p.N, client.DeserializeRule(p.Rule));
                }
            }

            public override DynamicTableEntity CreateTableEntity()
            {
                var entity = base.CreateTableEntity();
                var outpoints = _MatchedRulesByInputOutpointIndex.Select(kv => new UIntPair()
                {
                    N = kv.Key,
                    Rule = kv.Value.ToString()
                }).ToArray();

                var ns = _MatchedRulesByOutputIndex.Select(kv => new UIntPair()
                {
                    N = kv.Key,
                    Rule = kv.Value.ToString()
                }).ToArray();

                MemoryStream ms = new MemoryStream();
                BitcoinStream stream = new BitcoinStream(ms, true);
                stream.ReadWrite(ref outpoints);
                stream.ReadWrite(ref ns);
                var bytes = Helper.GetBytes(ms);
                Helper.SetEntityProperty(entity, "matches", bytes);
                return entity;
            }

            public string WalletId
            {
                get
                {
                    return BalanceId;
                }
            }
            protected override string CalculatePartitionKey()
            {
                var bytes = Encoding.UTF8.GetBytes(BalanceId);
                if (bytes.Length >= 3)
                    return Helper.GetPartitionKey(12, bytes, bytes.Length - 3, 3);
                if (bytes.Length == 2)
                    return Helper.GetPartitionKey(12, bytes, bytes.Length - 2, 2);
                if (bytes.Length == 1)
                    return Helper.GetPartitionKey(12, bytes, bytes.Length - 1, 1);
                return "00";
            }

            public static Dictionary<string, Entity> ExtractFromTransaction(uint256 blockId,
                                                                            Transaction tx,
                                                                            uint256 txId,
                                                                            WalletRuleEntryCollection walletCollection)
            {
                Dictionary<string, Entity> entitiesByWallet = new Dictionary<string, Entity>();
                var entryByAddress = AddressBalanceChangeEntry.Entity.ExtractFromTransaction(blockId, tx, txId);
                foreach (var entryAddress in entryByAddress)
                {
                    foreach (var walletRuleEntry in walletCollection.GetRulesForAddress(entryAddress.Key))
                    {
                        Entity walletEntity = null;
                        if (!entitiesByWallet.TryGetValue(walletRuleEntry.WalletId, out walletEntity))
                        {
                            walletEntity = new Entity(txId, walletRuleEntry.WalletId, blockId);
                            walletEntity.HasOpReturn = entryAddress.Value.HasOpReturn;
                            walletEntity.IsCoinbase = entryAddress.Value.IsCoinbase;
                            entitiesByWallet.Add(walletRuleEntry.WalletId, walletEntity);
                        }
                        walletEntity.ReceivedTxOutIndices.AddRange(entryAddress.Value.ReceivedTxOutIndices);
                        foreach (var n in entryAddress.Value.ReceivedTxOutIndices)
                        {
                            walletEntity.MatchedRulesByOutputIndex.AddOrReplace(n, walletRuleEntry.Rule);
                        }
                        uint currentIndex = (uint)walletEntity.SpentOutpoints.Count;
                        walletEntity.SpentOutpoints.AddRange(entryAddress.Value.SpentOutpoints);
                        for (int i = (int)currentIndex ; i < walletEntity.SpentOutpoints.Count ; i++)
                        {
                            walletEntity.MatchedRulesByInputOutpointIndex.AddOrReplace((uint)i, walletRuleEntry.Rule);
                        }
                    }
                }
                foreach (var wallet in entitiesByWallet)
                {
                    wallet.Value.SpentOutpoints.Distinct();
                    wallet.Value.ReceivedTxOutIndices.Distinct();
                }
                return entitiesByWallet;
            }

            private readonly Dictionary<uint, WalletRule> _MatchedRulesByOutputIndex = new Dictionary<uint, WalletRule>();
            public Dictionary<uint, WalletRule> MatchedRulesByOutputIndex
            {
                get
                {
                    return _MatchedRulesByOutputIndex;
                }
            }

            private readonly Dictionary<uint, WalletRule> _MatchedRulesByInputOutpointIndex = new Dictionary<uint, WalletRule>();
            public Dictionary<uint, WalletRule> MatchedRulesByInputOutpointIndex
            {
                get
                {
                    return _MatchedRulesByInputOutpointIndex;
                }
            }
        }


        public WalletRule GetMatchedRule(Spendable spendable)
        {
            return GetMatchedRule(spendable.OutPoint);
        }

        Dictionary<uint, WalletRule> _MatchedRulesByOutputIndex = new Dictionary<uint, WalletRule>();
        Dictionary<OutPoint, WalletRule> _MatchedRulesByOutpoint = new Dictionary<OutPoint, WalletRule>();
        public WalletRule GetMatchedRule(OutPoint outpoint)
        {
            if (outpoint.Hash == TransactionId)
            {
                WalletRule rule = null;
                _MatchedRulesByOutputIndex.TryGetValue(outpoint.N, out rule);
                return rule;
            }
            else
            {
                WalletRule rule = null;
                _MatchedRulesByOutpoint.TryGetValue(outpoint, out rule);
                return rule;
            }
        }
    }
}
