using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Table;
using NBitcoin.DataEncoders;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace NBitcoin.Indexer
{
    public class BalanceChangeEntry
    {
        public string BalanceId
        {
            get;
            set;
        }
        public BalanceChangeEntry(params Entity[] entities)
        {
            if (entities == null)
                throw new ArgumentNullException("entities");
            if (entities.Length == 0)
                throw new ArgumentException("At least one entity should be provided", "entities");

            var loadedEntity = entities.FirstOrDefault(e => e.IsLoaded);
            if (loadedEntity == null)
                loadedEntity = entities[0];

            BalanceId = loadedEntity.BalanceId;
            TransactionId = new uint256(loadedEntity.TransactionId);
            BlockIds = entities
                                    .Where(s => s.BlockId != null)
                                    .Select(s => new uint256(s.BlockId))
                                    .ToArray();
            SpentOutpoints = loadedEntity.SpentOutpoints;
            ReceivedTxOutIndices = loadedEntity.ReceivedTxOutIndices;
            if (loadedEntity.IsLoaded)
            {
                ReceivedCoins = new List<Spendable>();
                for (int i = 0 ; i < loadedEntity.ReceivedTxOutIndices.Count ; i++)
                {
                    ReceivedCoins.Add(new Spendable(new OutPoint(TransactionId, loadedEntity.ReceivedTxOutIndices[i]), loadedEntity.ReceivedTxOuts[i]));
                }
                SpentCoins = new List<Spendable>();
                for (int i = 0 ; i < SpentOutpoints.Count ; i++)
                {
                    SpentCoins.Add(new Spendable(SpentOutpoints[i], loadedEntity.SpentTxOuts[i]));
                }
                BalanceChange = ReceivedCoins.Select(t => t.TxOut.Value).Sum() - SpentCoins.Select(t => t.TxOut.Value).Sum();
            }
            MempoolDate = entities.Where(e => e.BlockId == null).Select(e => e.Timestamp).FirstOrDefault();
        }

        public abstract class Entity
        {
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


            public Entity()
            {

            }

            

            public Entity(DynamicTableEntity entity)
            {
                var splitted = entity.RowKey.Split('-');
                BalanceId = splitted[0];
                TransactionId = new uint256(splitted[1]);
                if (splitted.Length >= 3 && splitted[2] != string.Empty)
                    BlockId = new uint256(splitted[2]);
                Timestamp = entity.Timestamp;
                _PartitionKey = entity.PartitionKey;

                _SpentOutpoints = Helper.DeserializeList<OutPoint>(Helper.GetEntityProperty(entity, "a"));
                _SpentTxOuts = Helper.DeserializeList<TxOut>(Helper.GetEntityProperty(entity, "b"));
                _ReceivedTxOutIndices = Helper.DeserializeList<IntCompactVarInt>(Helper.GetEntityProperty(entity, "c"))
                                        .Select(o => (uint)o.ToLong())
                                        .ToList();
                _ReceivedTxOuts = Helper.DeserializeList<TxOut>(Helper.GetEntityProperty(entity, "d"));
            }

            public DynamicTableEntity CreateTableEntity()
            {
                DynamicTableEntity entity = new DynamicTableEntity();
                entity.ETag = "*";
                entity.PartitionKey = PartitionKey;
                entity.RowKey = BalanceId + "-" + TransactionId + "-" + BlockId;
                Helper.SetEntityProperty(entity, "a", Helper.SerializeList(SpentOutpoints));
                Helper.SetEntityProperty(entity, "b", Helper.SerializeList(SpentTxOuts));
                Helper.SetEntityProperty(entity, "c", Helper.SerializeList(ReceivedTxOutIndices.Select(e => new IntCompactVarInt(e))));
                Helper.SetEntityProperty(entity, "d", Helper.SerializeList(ReceivedTxOuts));
                return entity;
            }

            public string BalanceId
            {
                get;
                set;
            }

            string _PartitionKey;
            public string PartitionKey
            {
                get
                {
                    if (_PartitionKey == null && BalanceId != null)
                    {
                        _PartitionKey = CalculatePartitionKey();
                    }
                    return _PartitionKey;
                }
            }

            protected abstract string CalculatePartitionKey();


            public Entity(uint256 txid, string balanceId, uint256 blockId)
            {
                BalanceId = balanceId;
                TransactionId = txid;
                BlockId = blockId;
            }

            public uint256 TransactionId
            {
                get;
                set;
            }
            public uint256 BlockId
            {
                get;
                set;
            }



            private readonly List<uint> _ReceivedTxOutIndices = new List<uint>();
            public List<uint> ReceivedTxOutIndices
            {
                get
                {
                    return _ReceivedTxOutIndices;
                }
            }

            private readonly List<TxOut> _SpentTxOuts = new List<TxOut>();
            public List<TxOut> SpentTxOuts
            {
                get
                {
                    return _SpentTxOuts;
                }
            }

            private readonly List<OutPoint> _SpentOutpoints = new List<OutPoint>();
            public List<OutPoint> SpentOutpoints
            {
                get
                {
                    return _SpentOutpoints;
                }
            }
            private readonly List<TxOut> _ReceivedTxOuts = new List<TxOut>();
            public List<TxOut> ReceivedTxOuts
            {
                get
                {
                    return _ReceivedTxOuts;
                }
            }
            public override string ToString()
            {
                return "RowKey : " + BalanceId;
            }

            public bool IsLoaded
            {
                get
                {
                    return SpentOutpoints.Count == SpentTxOuts.Count && ReceivedTxOuts.Count == ReceivedTxOutIndices.Count;
                }
            }

            public DateTimeOffset? Timestamp
            {
                get;
                set;
            }

        }

        ChainedBlock _ConfirmedBlock;
        bool _ConfirmedSet;
        public ChainedBlock ConfirmedBlock
        {
            get
            {
                if (!_ConfirmedSet)
                    throw new InvalidOperationException("You need to call FetchConfirmedBlock(Chain chain) to attach the confirmed block to this entry");
                return _ConfirmedBlock;
            }
            private set
            {
                _ConfirmedSet = true;
                _ConfirmedBlock = value;
            }
        }
        /// <summary>
        /// Fetch ConfirmationInfo if not already set about this entry from local chain
        /// </summary>
        /// <param name="chain">Local chain</param>
        /// <returns>Returns this</returns>
        public BalanceChangeEntry FetchConfirmedBlock(Chain chain)
        {
            if (_ConfirmedBlock != null)
                return this;
            if (BlockIds == null || BlockIds.Length == 0)
                return this;
            ConfirmedBlock = BlockIds.Select(id => chain.GetBlock(id)).FirstOrDefault(b => b != null);
            return this;
        }
        public uint256 TransactionId
        {
            get;
            set;
        }

        

        List<Spendable> _ReceivedCoins;
        public List<Spendable> ReceivedCoins
        {
            get
            {
                return _ReceivedCoins;
            }
            set
            {
                _ReceivedCoins = value;
            }
        }

        List<OutPoint> _SpentOutpoints = new List<OutPoint>();

        /// <summary>
        /// List of spent outpoints
        /// </summary>
        public List<OutPoint> SpentOutpoints
        {
            get
            {
                return _SpentOutpoints;
            }
            set
            {
                _SpentOutpoints = value;
            }
        }


        List<Spendable> _SpentCoins;

        /// <summary>
        /// List of spent coins
        /// Can be null if the indexer have not yet indexed parent transactions
        /// Use SpentOutpoints if you only need outpoints
        /// </summary>
        public List<Spendable> SpentCoins
        {
            get
            {
                return _SpentCoins;
            }
            set
            {
                _SpentCoins = value;
            }
        }
        public List<int> TxOutIndices
        {
            get;
            set;
        }



        public uint256[] BlockIds
        {
            get;
            set;
        }

        public Money BalanceChange
        {
            get;
            set;
        }

        public override string ToString()
        {
            return BalanceId + " - " + (BalanceChange == null ? "??" : BalanceChange.ToString());
        }

        public DateTimeOffset? MempoolDate
        {
            get;
            set;
        }

        public List<uint> ReceivedTxOutIndices
        {
            get;
            set;
        }
    }
}
