using Microsoft.WindowsAzure.Storage.Table;
using NBitcoin.DataEncoders;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NBitcoin.Indexer
{
    public class OrderedBalanceChange
    {
        public static IEnumerable<OrderedBalanceChange> Extract(uint256 txId, Transaction transaction, uint256 blockId, BlockHeader blockHeader, int height)
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
                        entry = new OrderedBalanceChange(txId, signer.ScriptPubKey, blockId, blockHeader, height);
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
                    entry = new OrderedBalanceChange(txId, output.ScriptPubKey, blockId, blockHeader, height);
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

        public string BalanceId
        {
            get;
            set;
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
            _ReceivedCoins = new List<Coin>();
        }
        private readonly List<uint> _SpentIndices;
        public List<uint> SpentIndices
        {
            get
            {
                return _SpentIndices;
            }
        }

        private readonly List<OutPoint> _SpentOutpoints;
        public List<OutPoint> SpentOutpoints
        {
            get
            {
                return _SpentOutpoints;
            }
        }

        private readonly List<Coin> _ReceivedCoins;
        public List<Coin> ReceivedCoins
        {
            get
            {
                return _ReceivedCoins;
            }
        }


        private List<Coin> _SpentCoins;

        /// <summary>
        /// Might be null if parent transactions have not yet been indexed
        /// </summary>
        public List<Coin> SpentCoins
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

        internal OrderedBalanceChange(DynamicTableEntity entity)
        {
            BalanceId = entity.PartitionKey;
            var splitted = entity.RowKey.Split(new string[] { "-" }, StringSplitOptions.RemoveEmptyEntries);
            Height = Helper.StringToHeight(splitted[0]);
            if (Height == int.MaxValue)
            {
                TransactionId = new uint256(splitted[2]);
            }
            else
            {
                BlockId = new uint256(splitted[1]);
                TransactionId = new uint256(splitted[2]);
            }
            SeenUtc = entity.Properties["s"].DateTime.Value;

            _SpentOutpoints = Helper.DeserializeList<OutPoint>(Helper.GetEntityProperty(entity, "a"));

            if (entity.Properties.ContainsKey("b"))
                _SpentCoins = Helper.DeserializeList<Spendable>(Helper.GetEntityProperty(entity, "b")).Select(s => new Coin(s)).ToList();
            else if (_SpentOutpoints.Count == 0)
                _SpentCoins = new List<Coin>();

            _SpentIndices = Helper.DeserializeList<BalanceChangeEntry.Entity.IntCompactVarInt>(Helper.GetEntityProperty(entity, "ss")).Select(i => (uint)i.ToLong()).ToList();

            var receivedIndices = Helper.DeserializeList<BalanceChangeEntry.Entity.IntCompactVarInt>(Helper.GetEntityProperty(entity, "c")).Select(i => (uint)i.ToLong()).ToList();
            var receivedTxOuts = Helper.DeserializeList<TxOut>(Helper.GetEntityProperty(entity, "d"));

            _ReceivedCoins = new List<Coin>();
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
        }

        public OrderedBalanceChange(uint256 txId, Script scriptPubKey, uint256 blockId, BlockHeader blockHeader, int height)
            : this()
        {
            BlockId = blockId;
            SeenUtc = blockHeader == null ? DateTime.UtcNow : blockHeader.BlockTime.UtcDateTime;
            Height = blockHeader == null ? int.MaxValue : height;
            TransactionId = txId;
            BalanceId = Helper.EncodeScript(scriptPubKey);
        }

        internal DynamicTableEntity ToEntity()
        {
            DynamicTableEntity entity = new DynamicTableEntity();
            entity.ETag = "*";
            entity.PartitionKey = BalanceId;
            if (BlockId != null)
                entity.RowKey = Helper.HeightToString(Height) + "-" + BlockId + "-" + TransactionId;
            else
            {
                entity.RowKey = Helper.HeightToString(int.MaxValue) + "-" + ToString(SeenUtc) + "-" + TransactionId;
            }

            entity.Properties.Add("s", new EntityProperty(SeenUtc));
            Helper.SetEntityProperty(entity, "ss", Helper.SerializeList(SpentIndices.Select(e => new BalanceChangeEntry.Entity.IntCompactVarInt(e))));

            Helper.SetEntityProperty(entity, "a", Helper.SerializeList(SpentOutpoints));
            if (SpentCoins != null)
                Helper.SetEntityProperty(entity, "b", Helper.SerializeList(SpentCoins.Select(c => new Spendable(c.Outpoint, c.TxOut))));
            Helper.SetEntityProperty(entity, "c", Helper.SerializeList(ReceivedCoins.Select(e => new BalanceChangeEntry.Entity.IntCompactVarInt(e.Outpoint.N))));
            Helper.SetEntityProperty(entity, "d", Helper.SerializeList(ReceivedCoins.Select(e => e.TxOut)));
            var flags = (HasOpReturn ? "o" : "n") + (IsCoinbase ? "o" : "n");
            entity.Properties.AddOrReplace("e", new EntityProperty(flags));

            return entity;
        }

        const string DateFormat = "yyyyMMddhhmmssff";
        private string ToString(DateTime date)
        {
            return Helper.ToggleChars(date.ToString(DateFormat));
        }

        public static IEnumerable<OrderedBalanceChange> Extract(Transaction tx)
        {
            return Extract(null, tx, null, null, 0);
        }
    }
}
