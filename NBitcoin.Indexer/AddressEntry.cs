using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace NBitcoin.Indexer
{
    public class AddressEntry
    {
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
        public AddressEntry FetchConfirmedBlock(Chain chain)
        {
            if (_ConfirmedBlock != null)
                return this;
            if (BlockIds == null || BlockIds.Length == 0)
                return this;
            ConfirmedBlock = BlockIds.Select(id => chain.GetBlock(id)).FirstOrDefault(b => b != null);
            return this;
        }
        public AddressEntry(Entity loadedEntity, params Entity[] otherEntities)
        {
            Address = Network.CreateFromBase58Data<BitcoinAddress>(loadedEntity.Address);
            TransactionId = new uint256(loadedEntity.TransactionId);
            BlockIds = otherEntities
                                    .Where(s => !string.IsNullOrEmpty(s.BlockId))
                                    .Select(s => new uint256(s.BlockId)).ToArray();
            ReceivedTxOuts = loadedEntity.GetReceivedTxOut();
            ReceivedTxInIndex = loadedEntity.GetReceivedOutput();

            PreviousOutpoints = loadedEntity.GetPreviousOutpoints();
            PreviousTxOuts = loadedEntity.GetPreviousTxOuts();
            MempoolDate = otherEntities.Where(e => string.IsNullOrEmpty(e.BlockId)).Select(e => e.Timestamp).FirstOrDefault();
            BalanceChange = (loadedEntity.PreviousTxOuts == null || loadedEntity.ReceivedTxOuts == null) ? null : ReceivedTxOuts.Select(t => t.Value).Sum() - PreviousTxOuts.Select(t => t.Value).Sum();
        }
        public class Entity : TableEntity
        {
            public static Dictionary<string, Entity> ExtractFromTransaction(Transaction tx, string txId)
            {
                return ExtractFromTransaction(null, tx, txId);
            }
            public static Dictionary<string, Entity> ExtractFromTransaction(Transaction tx, uint256 txId)
            {
                return ExtractFromTransaction(null, tx, txId);
            }
            public static Dictionary<string, Entity> ExtractFromTransaction(uint256 blockId, Transaction tx, uint256 txId)
            {
                return ExtractFromTransaction(blockId == null ? null : blockId.ToString(), tx, txId == null ? null : txId.ToString());
            }
            public static Dictionary<string, Entity> ExtractFromTransaction(string blockId, Transaction tx, string txId)
            {
                if (txId == null)
                    txId = tx.GetHash().ToString();
                Dictionary<string, AddressEntry.Entity> entryByAddress = new Dictionary<string, AddressEntry.Entity>();
                foreach (var input in tx.Inputs)
                {
                    if (tx.IsCoinBase)
                        break;
                    var signer = input.ScriptSig.GetSignerAddress(AzureIndexer.InternalNetwork);
                    if (signer != null)
                    {
                        AddressEntry.Entity entry = null;
                        if (!entryByAddress.TryGetValue(signer.ToString(), out entry))
                        {
                            entry = new AddressEntry.Entity(txId, signer, blockId);
                            entryByAddress.Add(signer.ToString(), entry);
                        }
                        entry.AddSend(input.PrevOut);
                    }
                }

                int i = 0;
                foreach (var output in tx.Outputs)
                {
                    var receiver = output.ScriptPubKey.GetDestinationAddress(AzureIndexer.InternalNetwork);
                    if (receiver != null)
                    {
                        AddressEntry.Entity entry = null;
                        if (!entryByAddress.TryGetValue(receiver.ToString(), out entry))
                        {
                            entry = new AddressEntry.Entity(txId, receiver, blockId);
                            entryByAddress.Add(receiver.ToString(), entry);
                        }
                        entry.AddReceive(i);
                    }
                    i++;
                }
                foreach (var kv in entryByAddress)
                    kv.Value.Flush();
                return entryByAddress;
            }

            public Entity()
            {

            }
            public Entity(string txid, BitcoinAddress address, string blockId)
            {
                var wif = address.ToString();
                PartitionKey = GetPartitionKey(wif);
                RowKey = wif + "-" + txid + "-" + blockId;
            }

            public static string GetPartitionKey(string wif)
            {
                char[] c = new char[3];
                c[0] = (int)(wif[wif.Length - 3]) % 2 == 0 ? 'a' : 'b';
                c[1] = wif[wif.Length - 2];
                c[2] = wif[wif.Length - 1];
                return new string(c);
            }

            MemoryStream receiveStream = new MemoryStream();
            void AddReceive(int n)
            {
                var nCompact = new CompactVarInt((ulong)n, 4);
                nCompact.ReadWrite(receiveStream, true);
            }

            MemoryStream outpointStream = new MemoryStream();
            void AddSend(OutPoint outpoint)
            {
                outpoint.ReadWrite(outpointStream, true);
            }
            void Flush()
            {
                AllSentOutpoints = Helper.GetBytes(outpointStream, false);
                AllReceivedOutput = Helper.GetBytes(receiveStream, false);
            }


            byte[] _AllReceivedOutput;
            [IgnoreProperty]
            public byte[] AllReceivedOutput
            {
                get
                {
                    if (_AllReceivedOutput == null)
                        _AllReceivedOutput = Helper.Concat(ReceivedOutput, ReceivedOutput1, ReceivedOutput2, ReceivedOutput3);
                    return _AllReceivedOutput;
                }
                set
                {
                    _AllReceivedOutput = value;
                    Helper.Spread(value, 1024 * 63, ref _ReceivedOutput, ref _ReceivedOutput1, ref _ReceivedOutput2, ref _ReceivedOutput3);
                }
            }

            byte[] _ReceivedOutput;
            public byte[] ReceivedOutput
            {
                get
                {
                    return _ReceivedOutput;
                }
                set
                {
                    _ReceivedOutput = value;
                }
            }

            byte[] _ReceivedOutput1;
            public byte[] ReceivedOutput1
            {
                get
                {
                    return _ReceivedOutput1;
                }
                set
                {
                    _ReceivedOutput1 = value;
                }
            }

            byte[] _ReceivedOutput2;
            public byte[] ReceivedOutput2
            {
                get
                {
                    return _ReceivedOutput2;
                }
                set
                {
                    _ReceivedOutput2 = value;
                }
            }

            byte[] _ReceivedOutput3;
            public byte[] ReceivedOutput3
            {
                get
                {
                    return _ReceivedOutput3;
                }
                set
                {
                    _ReceivedOutput3 = value;
                }
            }

            public string Address
            {
                get
                {
                    return RowKey.Split('-')[0];
                }
            }

            public string TransactionId
            {
                get
                {
                    return RowKey.Split('-')[1];
                }
            }
            public string BlockId
            {
                get
                {
                    var splitted = RowKey.Split('-');
                    if (splitted.Length < 3)
                        return "";
                    return RowKey.Split('-')[2];
                }
            }

            byte[] _AllSentOutpoints;
            [IgnoreProperty]
            public byte[] AllSentOutpoints
            {
                get
                {
                    if (_AllSentOutpoints == null)
                        _AllSentOutpoints = Helper.Concat(_SentOutpoints, _SentOutpoints1, _SentOutpoints2, _SentOutpoints3);
                    return _AllSentOutpoints;
                }
                set
                {
                    _AllSentOutpoints = value;
                    Helper.Spread(value, 1024 * 63, ref _SentOutpoints, ref _SentOutpoints1, ref _SentOutpoints2, ref _SentOutpoints3);
                }
            }

            byte[] _SentOutpoints;
            public byte[] SentOutpoints
            {
                get
                {
                    return _SentOutpoints;
                }
                set
                {
                    _SentOutpoints = value;
                }
            }

            byte[] _SentOutpoints1;
            public byte[] SentOutpoints1
            {
                get
                {
                    return _SentOutpoints1;
                }
                set
                {
                    _SentOutpoints1 = value;
                }
            }

            byte[] _SentOutpoints2;
            public byte[] SentOutpoints2
            {
                get
                {
                    return _SentOutpoints2;
                }
                set
                {
                    _SentOutpoints2 = value;
                }
            }

            byte[] _SentOutpoints3;
            public byte[] SentOutpoints3
            {
                get
                {
                    return _SentOutpoints3;
                }
                set
                {
                    _SentOutpoints3 = value;
                }
            }

            public List<int> GetReceivedOutput()
            {
                List<int> indexes = new List<int>();
                if (AllReceivedOutput == null)
                    return indexes;
                MemoryStream ms = new MemoryStream(AllReceivedOutput);
                ms.Position = 0;
                while (ms.Position != ms.Length)
                {
                    CompactVarInt value = new CompactVarInt(4);
                    value.ReadWrite(ms, false);
                    indexes.Add((int)value.ToLong());
                }
                return indexes;
            }

            public List<OutPoint> GetPreviousOutpoints()
            {
                return Helper.DeserializeList<OutPoint>(AllSentOutpoints);
            }


            internal List<TxOut> GetReceivedTxOut()
            {
                return Helper.DeserializeList<TxOut>(ReceivedTxOuts);
            }


            public override string ToString()
            {
                return "RowKey : " + RowKey;
            }

            byte[] _PreviousTxOuts;
            [IgnoreProperty]
            public byte[] PreviousTxOuts
            {
                get
                {
                    if (_PreviousTxOuts == null)
                        _PreviousTxOuts = Helper.Concat(SentTxOuts1, SentTxOuts2, SentTxOuts3, SentTxOuts4);
                    return _PreviousTxOuts;
                }
                set
                {
                    _PreviousTxOuts = value;
                    Helper.Spread(value, 1024 * 63, ref _SentTxOuts1, ref _SentTxOuts2, ref _SentTxOuts3, ref _SentTxOuts4);
                }
            }

            byte[] _SentTxOuts1;
            public byte[] SentTxOuts1
            {
                get
                {
                    return _SentTxOuts1;
                }
                set
                {
                    _SentTxOuts1 = value;
                }
            }
            byte[] _SentTxOuts2;
            public byte[] SentTxOuts2
            {
                get
                {
                    return _SentTxOuts2;
                }
                set
                {
                    _SentTxOuts2 = value;
                }
            }
            byte[] _SentTxOuts3;
            public byte[] SentTxOuts3
            {
                get
                {
                    return _SentTxOuts3;
                }
                set
                {
                    _SentTxOuts3 = value;
                }
            }
            byte[] _SentTxOuts4;
            public byte[] SentTxOuts4
            {
                get
                {
                    return _SentTxOuts4;
                }
                set
                {
                    _SentTxOuts4 = value;
                }
            }

            byte[] _ReceivedTxOuts;

            [IgnoreProperty]
            public bool Loaded
            {
                get
                {
                    return ReceivedTxOuts != null;
                }
            }

            [IgnoreProperty]
            public byte[] ReceivedTxOuts
            {
                get
                {
                    if (_ReceivedTxOuts == null)
                        _ReceivedTxOuts = Helper.Concat(_ReceivedTxOuts1, _ReceivedTxOuts2, _ReceivedTxOuts3, _ReceivedTxOuts4);
                    return _ReceivedTxOuts;
                }
                set
                {
                    _ReceivedTxOuts = value;
                    Helper.Spread(value, 1024 * 63, ref _ReceivedTxOuts1, ref _ReceivedTxOuts2, ref _ReceivedTxOuts3, ref _ReceivedTxOuts4);
                }
            }

            byte[] _ReceivedTxOuts1;
            public byte[] ReceivedTxOuts1
            {
                get
                {
                    return _ReceivedTxOuts1;
                }
                set
                {
                    _ReceivedTxOuts1 = value;
                }
            }
            byte[] _ReceivedTxOuts2;
            public byte[] ReceivedTxOuts2
            {
                get
                {
                    return _ReceivedTxOuts2;
                }
                set
                {
                    _ReceivedTxOuts2 = value;
                }
            }
            byte[] _ReceivedTxOuts3;
            public byte[] ReceivedTxOuts3
            {
                get
                {
                    return _ReceivedTxOuts3;
                }
                set
                {
                    _ReceivedTxOuts3 = value;
                }
            }

            byte[] _ReceivedTxOuts4;
            public byte[] ReceivedTxOuts4
            {
                get
                {
                    return _ReceivedTxOuts4;
                }
                set
                {
                    _ReceivedTxOuts4 = value;
                }
            }
            internal List<TxOut> GetPreviousTxOuts()
            {
                return Helper.DeserializeList<TxOut>(PreviousTxOuts);
            }
        }
        public uint256 TransactionId
        {
            get;
            set;
        }

        public BitcoinAddress Address
        {
            get;
            set;
        }

        public List<OutPoint> PreviousOutpoints
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

        public List<TxOut> ReceivedTxOuts
        {
            get;
            set;
        }
        public List<TxOut> PreviousTxOuts
        {
            get;
            set;
        }

        public override string ToString()
        {
            return Address + " - " + (BalanceChange == null ? "??" : BalanceChange.ToString());
        }

        public IEnumerable<OutPoint> ReceivedOutpoints
        {
            get
            {
                return ReceivedTxInIndex.Select(i => new OutPoint(TransactionId, i));
            }
        }
        public List<int> ReceivedTxInIndex
        {
            get;
            set;
        }

        public DateTimeOffset? MempoolDate
        {
            get;
            set;
        }
    }
}
