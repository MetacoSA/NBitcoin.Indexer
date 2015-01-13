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

        public class Entity
        {
            public enum TransactionEntryType
            {
                Mempool,
                ConfirmedTransaction,
                Colored
            }
            public Entity(uint256 txId, Transaction tx, uint256 blockId)
            {
                if (txId == null)
                    txId = tx.GetHash();
                TxId = txId;
                Transaction = tx;
                BlockId = blockId;
                if (blockId == null)
                    Type = TransactionEntryType.Mempool;
                else
                    Type = TransactionEntryType.ConfirmedTransaction;
            }

            public Entity(uint256 txId)
            {
                if (txId == null)
                    throw new ArgumentNullException("txId");
                TxId = txId;
            }

            public Entity(DynamicTableEntity entity)
            {
                var splitted = entity.RowKey.Split(new string[] { "-" }, StringSplitOptions.None);
                _PartitionKey = entity.PartitionKey;
                Timestamp = entity.Timestamp;
                TxId = new uint256(splitted[0]);
                Type = GetType(splitted[1]);
                if (splitted.Length >= 3 && splitted[2] != string.Empty)
                    BlockId = new uint256(splitted[2]);
                var bytes = Helper.GetEntityProperty(entity, "a");
                if (bytes != null && bytes.Length != 0)
                {
                    Transaction = new Transaction();
                    Transaction.ReadWrite(bytes);
                }
                bytes = Helper.GetEntityProperty(entity, "b");
                if (bytes != null && bytes.Length != 0)
                {
                    ColoredTransaction = new ColoredTransaction();
                    ColoredTransaction.ReadWrite(bytes);
                }
                _PreviousTxOuts = Helper.DeserializeList<TxOut>(Helper.GetEntityProperty(entity, "c"));
            }

            public DynamicTableEntity CreateTableEntity()
            {
                var entity = new DynamicTableEntity();
                entity.ETag = "*";
                entity.PartitionKey = PartitionKey;
                entity.RowKey = TxId + "-" + TypeLetter + "-" + BlockId;
                if (Transaction != null)
                    Helper.SetEntityProperty(entity, "a", Transaction.ToBytes());
                if (ColoredTransaction != null)
                    Helper.SetEntityProperty(entity, "b", ColoredTransaction.ToBytes());
                Helper.SetEntityProperty(entity, "c", Helper.SerializeList(PreviousTxOuts));
                return entity;
            }

            public string TypeLetter
            {
                get
                {
                    return Type == TransactionEntryType.Colored ? "c" :
                        Type == TransactionEntryType.ConfirmedTransaction ? "b" :
                        Type == TransactionEntryType.Mempool ? "m" : "?";
                }
            }
            public TransactionEntryType GetType(string letter)
            {
                switch (letter[0])
                {
                    case 'c':
                        return TransactionEntryType.Colored;
                    case 'b':
                        return TransactionEntryType.ConfirmedTransaction;
                    case 'm':
                        return TransactionEntryType.Mempool;
                    default:
                        return TransactionEntryType.Mempool;
                }
            }

            string _PartitionKey;
            public string PartitionKey
            {
                get
                {
                    if (_PartitionKey == null && TxId != null)
                    {
                        _PartitionKey = Helper.GetPartitionKey(10, new[] { TxId.GetByte(0), TxId.GetByte(1) }, 0, 2);
                    }
                    return _PartitionKey;
                }
            }

            public DateTimeOffset? Timestamp
            {
                get;
                set;
            }

            public Entity(uint256 txId, ColoredTransaction colored)
            {
                if (txId == null)
                    throw new ArgumentNullException("txId");
                TxId = txId;
                ColoredTransaction = colored;
                Type = TransactionEntryType.Colored;
            }


            public bool IsLoaded
            {
                get
                {
                    return Transaction != null &&
                        (Transaction.IsCoinBase || (PreviousTxOuts.Count == Transaction.Inputs.Count));
                }
            }

            public uint256 BlockId
            {
                get;
                set;
            }


            public uint256 TxId
            {
                get;
                set;
            }

            public ColoredTransaction ColoredTransaction
            {
                get;
                set;
            }


            public Transaction Transaction
            {
                get;
                set;
            }


            readonly List<TxOut> _PreviousTxOuts = new List<TxOut>();
            public List<TxOut> PreviousTxOuts
            {
                get
                {
                    return _PreviousTxOuts;
                }
            }


            public TransactionEntryType Type
            {
                get;
                set;
            }
        }
        internal TransactionEntry(Entity[] entities)
        {
            TransactionId = entities[0].TxId;
            BlockIds = entities.Select(e => e.BlockId).Where(b => b != null).ToArray();
            MempoolDate = entities.Where(e => e.Type == Entity.TransactionEntryType.Mempool)
                                  .Select(e => e.Timestamp)
                                  .Min();

            var loadedEntity = entities.FirstOrDefault(e => e.Transaction != null && e.IsLoaded);
            if (loadedEntity == null)
                loadedEntity = entities.FirstOrDefault(e => e.Transaction != null);
            if (loadedEntity != null)
            {
                Transaction = loadedEntity.Transaction;

                if (loadedEntity.IsLoaded && !loadedEntity.Transaction.IsCoinBase)
                {
                    SpentCoins = new List<Spendable>();
                    for (int i = 0 ; i < Transaction.Inputs.Count ; i++)
                    {
                        SpentCoins.Add(new Spendable(Transaction.Inputs[i].PrevOut, loadedEntity.PreviousTxOuts[i]));
                    }
                }
            }

            var coloredLoadedEntity = entities.FirstOrDefault(e => e.ColoredTransaction != null);
            if (coloredLoadedEntity != null)
            {
                ColoredTransaction = coloredLoadedEntity.ColoredTransaction;
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
                if (SpentCoins == null || Transaction == null)
                    return null;
                if (Transaction.IsCoinBase)
                    return Money.Zero;
                return SpentCoins.Select(o => o.TxOut.Value).Sum() - Transaction.TotalOut;
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

        /// <summary>
        /// Coins spent during the transaction, can be null if the indexer miss parent transactions
        /// </summary>
        public List<Spendable> SpentCoins
        {
            get;
            set;
        }
    }
}
