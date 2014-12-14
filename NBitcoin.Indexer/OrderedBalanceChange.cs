using Microsoft.WindowsAzure.Storage.Table;
using NBitcoin.DataEncoders;
using NBitcoin.Indexer.DamienG.Security.Cryptography;
using NBitcoin.OpenAsset;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NBitcoin.Indexer
{
    public class OrderedBalanceChange
    {
        public static IEnumerable<OrderedBalanceChange> ExtractScriptBalances(uint256 txId, Transaction transaction, uint256 blockId, BlockHeader blockHeader, int height)
        {
            if (transaction == null)
                throw new ArgumentNullException("transaction");
            if (txId == null)
                txId = transaction.GetHash();

            if (blockId == null && blockHeader != null)
                blockId = blockHeader.GetHash();

            Dictionary<Script, OrderedBalanceChange> changeByScriptPubKey = new Dictionary<Script, OrderedBalanceChange>();
            uint i = 0;
            foreach (var input in transaction.Inputs)
            {
                if (transaction.IsCoinBase)
                {
                    i++;
                    break;
                }
                var signer = input.ScriptSig.GetSigner();
                if (signer != null)
                {
                    OrderedBalanceChange entry = null;
                    if (!changeByScriptPubKey.TryGetValue(signer.ScriptPubKey, out entry))
                    {
                        entry = new OrderedBalanceChange(txId, OrderedBalanceChange.GetBalanceId(signer.ScriptPubKey), blockId, blockHeader, height);
                        changeByScriptPubKey.Add(signer.ScriptPubKey, entry);
                    }
                    entry.SpentOutpoints.Add(input.PrevOut);
                    entry.SpentIndices.Add(i);
                }
                i++;
            }

            i = 0;
            bool hasOpReturn = false;
            foreach (var output in transaction.Outputs)
            {
                if (TxNullDataTemplate.Instance.CheckScriptPubKey(output.ScriptPubKey))
                {
                    hasOpReturn = true;
                    i++;
                    continue;
                }

                OrderedBalanceChange entry = null;
                if (!changeByScriptPubKey.TryGetValue(output.ScriptPubKey, out entry))
                {
                    entry = new OrderedBalanceChange(txId, GetBalanceId(output.ScriptPubKey), blockId, blockHeader, height);
                    changeByScriptPubKey.Add(output.ScriptPubKey, entry);
                }
                entry.ReceivedCoins.Add(new Coin()
                {
                    Outpoint = new OutPoint(txId, i),
                    TxOut = output
                });
                i++;
            }

            foreach (var entity in changeByScriptPubKey)
            {
                entity.Value.HasOpReturn = hasOpReturn;
                entity.Value.IsCoinbase = transaction.IsCoinBase;
            }

            return changeByScriptPubKey.Values;
        }

        public static IEnumerable<OrderedBalanceChange> ExtractWalletBalances(
                                                                            uint256 txId,
                                                                            Transaction tx,
                                                                            uint256 blockId,
                                                                            BlockHeader blockHeader,
                                                                            int height,
                                                                            WalletRuleEntryCollection walletCollection)
        {
            Dictionary<string, OrderedBalanceChange> entitiesByWallet = new Dictionary<string, OrderedBalanceChange>();
            var scriptBalances = ExtractScriptBalances(txId, tx, blockId, blockHeader, height);
            foreach (var scriptBalance in scriptBalances)
            {
                foreach (var walletRuleEntry in walletCollection.GetRulesFor(scriptBalance.GetScript()))
                {
                    OrderedBalanceChange walletEntity = null;
                    if (!entitiesByWallet.TryGetValue(walletRuleEntry.WalletId, out walletEntity))
                    {
                        walletEntity = new OrderedBalanceChange(txId, OrderedBalanceChange.GetBalanceId(walletRuleEntry.WalletId), blockId, blockHeader, height);
                        walletEntity.HasOpReturn = scriptBalance.HasOpReturn;
                        walletEntity.IsCoinbase = scriptBalance.IsCoinbase;
                        entitiesByWallet.Add(walletRuleEntry.WalletId, walletEntity);
                    }
                    walletEntity.Merge(scriptBalance, walletRuleEntry.Rule);
                }
            }
            return entitiesByWallet.Values;
        }


        private readonly List<MatchedRule> _MatchedRules = new List<MatchedRule>();
        public List<MatchedRule> MatchedRules
        {
            get
            {
                return _MatchedRules;
            }
        }

        private void Merge(OrderedBalanceChange other, WalletRule walletRule)
        {
            if (other.ReceivedCoins.Count != 0)
            {
                ReceivedCoins.AddRange(other.ReceivedCoins);
                ReceivedCoins = new CoinCollection(ReceivedCoins.Distinct<Coin, OutPoint>(c => c.Outpoint));
                foreach (var c in other.ReceivedCoins)
                {
                    this.MatchedRules.Add(new MatchedRule()
                    {
                        Index = c.Outpoint.N,
                        Rule = walletRule,
                        MatchType = MatchLocation.Output
                    });
                }
            }

            if (other.SpentIndices.Count != 0)
            {
                SpentIndices.AddRange(other.SpentIndices);
                SpentIndices = SpentIndices.Distinct().ToList();

                SpentOutpoints.AddRange(other.SpentOutpoints);
                SpentOutpoints = SpentOutpoints.Distinct().ToList();

                if (other.SpentCoins != null)
                {
                    if (SpentCoins == null)
                        SpentCoins = new CoinCollection();
                    SpentCoins.AddRange(other.SpentCoins);
                    SpentCoins = new CoinCollection(SpentCoins.Distinct<Coin, OutPoint>(c => c.Outpoint).ToList());
                }

                foreach (var c in other.SpentIndices)
                {
                    this.MatchedRules.Add(new MatchedRule()
                    {
                        Index = c,
                        Rule = walletRule,
                        MatchType = MatchLocation.Input
                    });
                }
            }
        }


        string _BalanceId;
        public string BalanceId
        {
            get
            {
                return _BalanceId;
            }
            set
            {
                _PartitionKey = null;
                _BalanceId = value;
            }
        }

        string _PartitionKey;
        public string PartitionKey
        {
            get
            {
                if (_PartitionKey == null)
                    _PartitionKey = OrderedBalanceChange.GetPartitionKey(BalanceId);
                return _PartitionKey;
            }
        }

        public int Height
        {
            get;
            set;
        }
        public uint256 BlockId
        {
            get;
            set;
        }
        public uint256 TransactionId
        {
            get;
            set;
        }
        public bool HasOpReturn
        {
            get;
            set;
        }

        public bool IsCoinbase
        {
            get;
            set;
        }

        public DateTime SeenUtc
        {
            get;
            set;
        }

        public OrderedBalanceChange()
        {
            _SpentIndices = new List<uint>();
            _SpentOutpoints = new List<OutPoint>();
            _ReceivedCoins = new CoinCollection();
        }
        private List<uint> _SpentIndices;
        public List<uint> SpentIndices
        {
            get
            {
                return _SpentIndices;
            }
            private set
            {
                _SpentIndices = value;
            }
        }

        private List<OutPoint> _SpentOutpoints;
        public List<OutPoint> SpentOutpoints
        {
            get
            {
                return _SpentOutpoints;
            }
            private set
            {
                _SpentOutpoints = value;
            }
        }

        private CoinCollection _ReceivedCoins;
        public CoinCollection ReceivedCoins
        {
            get
            {
                return _ReceivedCoins;
            }
            private set
            {
                _ReceivedCoins = value;
            }
        }


        private CoinCollection _SpentCoins;

        /// <summary>
        /// Might be null if parent transactions have not yet been indexed
        /// </summary>
        public CoinCollection SpentCoins
        {
            get
            {
                return _SpentCoins;
            }
            internal set
            {
                _SpentCoins = value;
            }
        }

        Money _Amount;
        public Money Amount
        {
            get
            {
                if (_Amount == null && _SpentCoins != null)
                {
                    _Amount = _ReceivedCoins.Select(c => c.Amount).Sum() - _SpentCoins.Select(c => c.Amount).Sum();
                }
                return _Amount;
            }
        }

        internal OrderedBalanceChange(DynamicTableEntity entity, JsonSerializerSettings settings)
        {
            var splitted = entity.RowKey.Split(new string[] { "-" }, StringSplitOptions.RemoveEmptyEntries);
            Height = Helper.StringToHeight(splitted[1]);
            BalanceId = splitted[0];
            if (Height == int.MaxValue)
            {
                TransactionId = new uint256(splitted[2]);
            }
            else
            {
                BlockId = new uint256(splitted[2]);
                TransactionId = new uint256(splitted[3]);
            }
            SeenUtc = entity.Properties["s"].DateTime.Value;

            _SpentOutpoints = Helper.DeserializeList<OutPoint>(Helper.GetEntityProperty(entity, "a"));

            if (entity.Properties.ContainsKey("b0"))
                _SpentCoins = new CoinCollection(Helper.DeserializeList<Spendable>(Helper.GetEntityProperty(entity, "b")).Select(s => new Coin(s)).ToList());
            else if (_SpentOutpoints.Count == 0)
                _SpentCoins = new CoinCollection();

            _SpentIndices = Helper.DeserializeList<IntCompactVarInt>(Helper.GetEntityProperty(entity, "ss")).Select(i => (uint)i.ToLong()).ToList();

            var receivedIndices = Helper.DeserializeList<IntCompactVarInt>(Helper.GetEntityProperty(entity, "c")).Select(i => (uint)i.ToLong()).ToList();
            var receivedTxOuts = Helper.DeserializeList<TxOut>(Helper.GetEntityProperty(entity, "d"));

            _ReceivedCoins = new CoinCollection();
            for (int i = 0 ; i < receivedIndices.Count ; i++)
            {
                _ReceivedCoins.Add(new Coin()
                {
                    Outpoint = new OutPoint(TransactionId, receivedIndices[i]),
                    TxOut = receivedTxOuts[i]
                });
            }

            var flags = entity.Properties["e"].StringValue;
            HasOpReturn = flags[0] == 'o';
            IsCoinbase = flags[1] == 'o';

            _MatchedRules = JsonConvert.DeserializeObject<List<MatchedRule>>(entity.Properties["f"].StringValue, settings).ToList();

            if(entity.Properties.ContainsKey("g"))
            {
                var ctx = new ColoredTransaction();
                ctx.FromBytes(entity["g"].BinaryValue);
                ColoredBalanceChangeEntry = new ColoredBalanceChangeEntry(this, ctx);
            }
        }

        public OrderedBalanceChange(uint256 txId, string balanceId, uint256 blockId, BlockHeader blockHeader, int height)
            : this()
        {
            BlockId = blockId;
            SeenUtc = blockHeader == null ? DateTime.UtcNow : blockHeader.BlockTime.UtcDateTime;
            Height = blockHeader == null ? int.MaxValue : height;
            TransactionId = txId;
            BalanceId = balanceId;
        }
        internal class IntCompactVarInt : CompactVarInt
        {
            public IntCompactVarInt(uint value)
                : base(value, 4)
            {
            }
            public IntCompactVarInt()
                : base(4)
            {

            }
        }

        internal DynamicTableEntity ToEntity(JsonSerializerSettings settings)
        {
            DynamicTableEntity entity = new DynamicTableEntity();
            entity.ETag = "*";
            entity.PartitionKey = PartitionKey;
            if (BlockId != null)
                entity.RowKey = BalanceId + "-" + Helper.HeightToString(Height) + "-" + BlockId + "-" + TransactionId;
            else
            {
                entity.RowKey = BalanceId + "-" + Helper.HeightToString(int.MaxValue) + "-" + TransactionId;
            }

            entity.Properties.Add("s", new EntityProperty(SeenUtc));
            Helper.SetEntityProperty(entity, "ss", Helper.SerializeList(SpentIndices.Select(e => new IntCompactVarInt(e))));

            Helper.SetEntityProperty(entity, "a", Helper.SerializeList(SpentOutpoints));
            if (SpentCoins != null)
                Helper.SetEntityProperty(entity, "b", Helper.SerializeList(SpentCoins.Select(c => new Spendable(c.Outpoint, c.TxOut))));
            Helper.SetEntityProperty(entity, "c", Helper.SerializeList(ReceivedCoins.Select(e => new IntCompactVarInt(e.Outpoint.N))));
            Helper.SetEntityProperty(entity, "d", Helper.SerializeList(ReceivedCoins.Select(e => e.TxOut)));
            var flags = (HasOpReturn ? "o" : "n") + (IsCoinbase ? "o" : "n");
            entity.Properties.AddOrReplace("e", new EntityProperty(flags));
            entity.Properties.AddOrReplace("f", new EntityProperty(JsonConvert.SerializeObject(MatchedRules, settings)));
            if (ColoredBalanceChangeEntry != null)
            {
                entity.Properties.AddOrReplace("g", new EntityProperty(ColoredBalanceChangeEntry._Colored.ToBytes()));
            }
            return entity;
        }

        public static string GetPartitionKey(string balanceId)
        {
            return Helper.GetPartitionKey(12, Crc32.Compute(balanceId));
        }

        const string DateFormat = "yyyyMMddhhmmssff";
        private string ToString(DateTime date)
        {
            return Helper.ToggleChars(date.ToString(DateFormat));
        }

        public static IEnumerable<OrderedBalanceChange> ExtractScriptBalances(Transaction tx)
        {
            return ExtractScriptBalances(null, tx, null, null, 0);
        }

        public static string GetBalanceId(Script scriptPubKey)
        {
            return Helper.EncodeScript(scriptPubKey);
        }

        public Script GetScript()
        {
            return Helper.DecodeScript(BalanceId);
        }

        public static string GetBalanceId(string walletId)
        {
            return "w" + Encoders.Hex.EncodeData(Encoding.UTF8.GetBytes(walletId));
        }

        public IEnumerable<WalletRule> GetMatchedRules(int index, MatchLocation matchType)
        {
            return MatchedRules.Where(r => r.Index == index && r.MatchType == matchType).Select(c => c.Rule);
        }


        public IEnumerable<WalletRule> GetMatchedRules(Coin coin)
        {
            return GetMatchedRules(coin.Outpoint);
        }

        public IEnumerable<WalletRule> GetMatchedRules(OutPoint outPoint)
        {
            if (outPoint.Hash == TransactionId)
                return GetMatchedRules((int)outPoint.N, MatchLocation.Output);
            else
            {
                var index = SpentOutpoints.IndexOf(outPoint);
                if (index == -1)
                    return new WalletRule[0];
                return GetMatchedRules((int)SpentIndices[index], MatchLocation.Input);
            }
        }

        public ColoredBalanceChangeEntry ColoredBalanceChangeEntry
        {
            get;
            set;
        }

        public bool MempoolEntry
        {
            get
            {
                return BlockId == null;
            }
        }
    }
}
