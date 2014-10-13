using Microsoft.WindowsAzure.Storage.Table;
using NBitcoin.Crypto;
using NBitcoin.OpenAsset;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NBitcoin.Indexer
{
    public class TransactionEntry
    {
        public class Entity : TableEntity
        {
            public Entity()
            {

            }

            public Entity(Transaction tx)
                : this(tx, null)
            {
            }

            private void SetTx(Transaction tx)
            {
                var transaction = tx.ToBytes();
                _txId = Hashes.Hash256(transaction);
                //if(transaction.Length < 1024 * 64)
                //	Transaction = transaction;
                Key = GetPartitionKeyUShort(_txId);
            }
            public Entity(Transaction tx, uint256 blockId)
            {
                SetTx(tx);
                if (blockId != null)
                    RowKey = _txId.ToString() + "-b" + blockId.ToString();
                else
                {
                    RowKey = _txId.ToString() + "-m";
                    TransactionBytes = tx.ToBytes();
                }
            }

            public Entity(uint256 txId, ColoredTransaction colored)
            {
                _txId = txId;
                Key = GetPartitionKeyUShort(txId);
                RowKey = txId.ToString() + "-c";
                ColoredTransactions = colored.ToBytes();
            }

            byte[] _Transaction;
            [IgnoreProperty]
            public byte[] TransactionBytes
            {
                get
                {
                    if (_Transaction == null)
                        _Transaction = Helper.Concat(Transaction, Transaction2, Transaction3, Transaction4);
                    return _Transaction;
                }
                set
                {
                    _Transaction = value;
                    Helper.Spread(value, 1024 * 63, ref _Transaction1, ref _Transaction2, ref _Transaction3, ref _Transaction4);
                }
            }

            byte[] _Transaction1;
            public byte[] Transaction
            {
                get
                {
                    return _Transaction1;
                }
                set
                {
                    _Transaction1 = value;
                }
            }
            byte[] _Transaction2;
            public byte[] Transaction2
            {
                get
                {
                    return _Transaction2;
                }
                set
                {
                    _Transaction2 = value;
                }
            }
            byte[] _Transaction3;
            public byte[] Transaction3
            {
                get
                {
                    return _Transaction3;
                }
                set
                {
                    _Transaction3 = value;
                }
            }
            byte[] _Transaction4;
            public byte[] Transaction4
            {
                get
                {
                    return _Transaction4;
                }
                set
                {
                    _Transaction4 = value;
                }
            }

            byte[] _AllSpentOutputs;
            [IgnoreProperty]
            public byte[] AllSpentOutputs
            {
                get
                {
                    if (_AllSpentOutputs == null)
                        _AllSpentOutputs = Helper.Concat(_SpentOutputs, _SpentOutputs1, _SpentOutputs2, _SpentOutputs3);
                    return _AllSpentOutputs;
                }
                set
                {
                    _AllSpentOutputs = value;
                    Helper.Spread(value, 1024 * 63, ref _SpentOutputs, ref _SpentOutputs1, ref _SpentOutputs2, ref _SpentOutputs3);
                }
            }

            byte[] _SpentOutputs;
            public byte[] SpentOutputs
            {
                get
                {
                    return _SpentOutputs;
                }
                set
                {
                    _SpentOutputs = value;
                }
            }
            byte[] _SpentOutputs1;
            public byte[] SpentOutputs1
            {
                get
                {
                    return _SpentOutputs1;
                }
                set
                {
                    _SpentOutputs1 = value;
                }
            }
            byte[] _SpentOutputs2;
            public byte[] SpentOutputs2
            {
                get
                {
                    return _SpentOutputs2;
                }
                set
                {
                    _SpentOutputs2 = value;
                }
            }

            byte[] _SpentOutputs3;
            public byte[] SpentOutputs3
            {
                get
                {
                    return _SpentOutputs3;
                }
                set
                {
                    _SpentOutputs3 = value;
                }
            }


            uint256 _txId;
            ushort? _Key;
            [IgnoreProperty]
            public ushort Key
            {
                get
                {
                    if (_Key == null)
                        _Key = ushort.Parse(PartitionKey, System.Globalization.NumberStyles.HexNumber);
                    return _Key.Value;
                }
                set
                {
                    PartitionKey = value.ToString("X2");
                    _Key = value;
                }
            }

            public byte[] ColoredTransactions
            {
                get;
                set;
            }
            public static string GetPartitionKey(uint256 txid)
            {
                var id = GetPartitionKeyUShort(txid);
                return id.ToString("X2");
            }

            private static ushort GetPartitionKeyUShort(uint256 txid)
            {
                return (ushort)((txid.GetByte(0) & 0xE0) + (txid.GetByte(1) << 8));
            }
            public override string ToString()
            {
                return PartitionKey + " " + RowKey;
            }
        }
        internal TransactionEntry(Entity[] entities)
        {
            List<uint256> blockIds = new List<uint256>();
            foreach (var entity in entities)
            {
                var parts = entity.RowKey.Split(new string[] { "-b" }, StringSplitOptions.RemoveEmptyEntries);
                if (parts.Length == 2)
                {
                    if (TransactionId == null)
                        TransactionId = new uint256(parts[0]);
                    blockIds.Add(new uint256(parts[1]));
                }
                else if (entity.RowKey.EndsWith("-m"))
                {
                    parts = entity.RowKey.Split(new string[] { "-m" }, StringSplitOptions.RemoveEmptyEntries);
                    if (TransactionId == null)
                        TransactionId = new uint256(parts[0]);
                    MempoolDate = entity.Timestamp;
                }
                else if (entity.RowKey.EndsWith("-c"))
                {

                }
            }
            BlockIds = blockIds.ToArray();
            var loadedEntity = entities.Where(e => e.TransactionBytes != null).FirstOrDefault();
            if (loadedEntity != null)
            {
                var bytes = loadedEntity.TransactionBytes;
                Transaction tx = new Transaction();
                tx.ReadWrite(bytes);
                Transaction = tx;

                if (loadedEntity.AllSpentOutputs != null)
                {
                    PreviousTxOuts = Helper.DeserializeList<TxOut>(loadedEntity.AllSpentOutputs).ToArray();
                }
            }

            var coloredLoadedEntity = entities.FirstOrDefault(e => e.ColoredTransactions != null);
            if (coloredLoadedEntity != null)
            {
                ColoredTransaction = new ColoredTransaction();
                ColoredTransaction.FromBytes(coloredLoadedEntity.ColoredTransactions);
            }
        }

        public ColoredTransaction ColoredTransaction
        {
            get;
            set;
        }

        public DateTimeOffset? MempoolDate
        {
            get;
            set;
        }

        public Money Fees
        {
            get
            {
                if (PreviousTxOuts == null || Transaction == null)
                    return null;
                return PreviousTxOuts.Select(o => o.Value).Sum() - Transaction.TotalOut;
            }
        }
        public uint256[] BlockIds
        {
            get;
            internal set;
        }
        public uint256 TransactionId
        {
            get;
            internal set;
        }
        public Transaction Transaction
        {
            get;
            internal set;
        }

        public TxOut[] PreviousTxOuts
        {
            get;
            set;
        }
    }
}
