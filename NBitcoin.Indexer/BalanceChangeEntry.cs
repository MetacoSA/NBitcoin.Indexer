using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Table;
using NBitcoin.DataEncoders;
using NBitcoin.OpenAsset;
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


        ColoredBalanceChangeEntry _ColoredBalanceChangeEntry;
        public ColoredBalanceChangeEntry ColoredBalanceChangeEntry
        {
            get
            {
                return _ColoredBalanceChangeEntry;
            }
            internal set
            {
                _ColoredBalanceChangeEntry = value;
            }
        }


        public void Init<TEntity>(params TEntity[] entities) where TEntity : BalanceChangeEntry.Entity
        {
            if (entities == null)
                throw new ArgumentNullException("entities");
            if (entities.Length == 0)
                throw new ArgumentException("At least one entity should be provided", "entities");

            var loadedEntity = entities.FirstOrDefault(e => e.IsLoaded);
            if (loadedEntity == null)
                loadedEntity = entities[0];
            HasOpReturn = loadedEntity.HasOpReturn;
            IsCoinbase = loadedEntity.IsCoinbase;
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
                ReceivedCoins = new SpendableCollection();
                for (int i = 0 ; i < loadedEntity.ReceivedTxOutIndices.Count ; i++)
                {
                    ReceivedCoins.Add(new Spendable(new OutPoint(TransactionId, loadedEntity.ReceivedTxOutIndices[i]), loadedEntity.ReceivedTxOuts[i]));
                }
                SpentCoins = new SpendableCollection();
                for (int i = 0 ; i < SpentOutpoints.Count ; i++)
                {
                    SpentCoins.Add(new Spendable(SpentOutpoints[i], loadedEntity.SpentTxOuts[i]));
                }
                BalanceChange = ReceivedCoins.Select(t => t.TxOut.Value).Sum() - SpentCoins.Select(t => t.TxOut.Value).Sum();
            }
            if (loadedEntity.ColorInformationData != null)
            {
                ColoredBalanceChangeEntry = new ColoredBalanceChangeEntry(this, loadedEntity.ColorInformationData);
            }
            MempoolDate = entities.Where(e => e.BlockId == null).Select(e => e.Timestamp).FirstOrDefault();
        }

        public abstract class Entity
        {
            public class ColorCoinInformation : IBitcoinSerializable
            {
                public ColorCoinInformation()
                {

                }
                public Asset Asset
                {
                    get
                    {
                        if (_AssetId == new uint160(0))
                            return null;
                        return new Asset(new AssetId(_AssetId), _Quantity);
                    }
                    set
                    {
                        if (value == null)
                        {
                            _AssetId = new uint160(0);
                            _Quantity = 0;
                        }
                        else
                        {
                            _AssetId = new uint160(value.Id.ToBytes(true));
                            _Quantity = value.Quantity;
                        }
                    }
                }

                uint160 _AssetId;
                ulong _Quantity;


                int _Transfer;
                public bool Transfer
                {
                    get
                    {
                        return _Transfer == 1;
                    }
                    set
                    {
                        _Transfer = value ? 1 : 0;
                    }
                }

                #region IBitcoinSerializable Members

                public void ReadWrite(BitcoinStream stream)
                {
                    stream.ReadWrite(ref _AssetId);
                    stream.ReadWrite(ref _Quantity);
                    stream.ReadWrite(ref _Transfer);
                }

                #endregion
            }
            public class ColorInformation : IBitcoinSerializable
            {
                List<ColorCoinInformation> _Inputs = new List<ColorCoinInformation>();
                public List<ColorCoinInformation> Inputs
                {
                    get
                    {
                        return _Inputs;
                    }
                    set
                    {
                        _Inputs = value;
                    }
                }
                List<ColorCoinInformation> _Outputs = new List<ColorCoinInformation>();
                public List<ColorCoinInformation> Outputs
                {
                    get
                    {
                        return _Outputs;
                    }
                    set
                    {
                        _Outputs = value;
                    }
                }



                #region IBitcoinSerializable Members

                public void ReadWrite(BitcoinStream stream)
                {
                    stream.ReadWrite(ref _Inputs);
                    stream.ReadWrite(ref _Outputs);
                }

                #endregion
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

                EntityProperty flagProperty = null;
                if (entity.Properties.TryGetValue("e", out flagProperty))
                {
                    var flags = flagProperty.StringValue;
                    HasOpReturn = flags[0] == 'o';
                    IsCoinbase = flags[1] == 'o';
                }

                EntityProperty colorInformationProperty = null;
                if (entity.Properties.TryGetValue("f", out colorInformationProperty))
                {
                    ColorInformationData = new ColorInformation();
                    ColorInformationData.FromBytes(colorInformationProperty.BinaryValue);
                }
            }

            public virtual DynamicTableEntity CreateTableEntity()
            {
                DynamicTableEntity entity = new DynamicTableEntity();
                entity.ETag = "*";
                entity.PartitionKey = PartitionKey;
                entity.RowKey = BalanceId + "-" + TransactionId + "-" + BlockId;
                Helper.SetEntityProperty(entity, "a", Helper.SerializeList(SpentOutpoints));
                Helper.SetEntityProperty(entity, "b", Helper.SerializeList(SpentTxOuts));
                Helper.SetEntityProperty(entity, "c", Helper.SerializeList(ReceivedTxOutIndices.Select(e => new IntCompactVarInt(e))));
                Helper.SetEntityProperty(entity, "d", Helper.SerializeList(ReceivedTxOuts));
                var flags = (HasOpReturn ? "o" : "n") + (IsCoinbase ? "o" : "n");
                entity.Properties.AddOrReplace("e", new EntityProperty(flags));
                if (ColorInformationData != null)
                {
                    entity.Properties.AddOrReplace("f", new EntityProperty(ColorInformationData.ToBytes()));
                }
                return entity;
            }

            public string BalanceId
            {
                get;
                set;
            }

            public ColorInformation ColorInformationData
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

            public bool IsCoinbase
            {
                get;
                set;
            }

            public bool HasOpReturn
            {
                get;
                set;
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
                _ConfirmedBlock = value;
            }
        }

        int _Confirmations;
        public int Confirmations
        {
            get
            {
                if (!_ConfirmedSet)
                    throw new InvalidOperationException("You need to call FetchConfirmedBlock(Chain chain) to attach the confirmed block to this entry");
                return _Confirmations;
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
            _ConfirmedSet = true;
            if (BlockIds == null || BlockIds.Length == 0)
                return this;
            ConfirmedBlock = BlockIds.Select(id => chain.GetBlock(id)).FirstOrDefault(b => b != null);
            if (ConfirmedBlock != null)
            {
                _Confirmations = chain.Height - ConfirmedBlock.Height + 1;
            }
            return this;
        }
        public uint256 TransactionId
        {
            get;
            set;
        }



        SpendableCollection _ReceivedCoins;
        public SpendableCollection ReceivedCoins
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


        SpendableCollection _SpentCoins;

        /// <summary>
        /// List of spent coins
        /// Can be null if the indexer have not yet indexed parent transactions
        /// Use SpentOutpoints if you only need outpoints
        /// </summary>
        public SpendableCollection SpentCoins
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


        public bool IsCoinbase
        {
            get;
            set;
        }
        public bool HasOpReturn
        {
            get;
            set;
        }
    }
}
