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
    public class AddressBalanceChangeEntry : BalanceChangeEntry
    {
        public AddressBalanceChangeEntry(params Entity[] entities)
        {
            Init(entities);
            if (entities.Length > 0)
                _Id = entities[0].Id;
        }

        TxDestination _Id;
        public TxDestination Id
        {
            get
            {
                if (_Id == null && BalanceId != null)
                {
                    _Id = Helper.DecodeId(BalanceId);
                }
                return _Id;
            }
        }

        public new AddressBalanceChangeEntry FetchConfirmedBlock(Chain chain)
        {
            return (AddressBalanceChangeEntry)base.FetchConfirmedBlock(chain);
        }
        public new class Entity : BalanceChangeEntry.Entity
        {
            public Entity(uint256 txid, TxDestination id, uint256 blockId)
                : base(txid, Helper.EncodeId(id), blockId)
            {
                _Id = id;
            }
            public Entity(DynamicTableEntity entity)
                : base(entity)
            {
            }

            public static Dictionary<string, Entity> ExtractFromTransaction(Transaction tx, uint256 txId)
            {
                return ExtractFromTransaction(null, tx, txId);
            }
            public static Dictionary<string, Entity> ExtractFromTransaction(uint256 blockId, Transaction tx, uint256 txId)
            {
                if (txId == null)
                    txId = tx.GetHash();
                Dictionary<string, AddressBalanceChangeEntry.Entity> entryByAddress = new Dictionary<string, AddressBalanceChangeEntry.Entity>();
                foreach (var input in tx.Inputs)
                {
                    if (tx.IsCoinBase)
                        break;
                    var signer = input.ScriptSig.GetSigner();
                    if (signer != null)
                    {
                        AddressBalanceChangeEntry.Entity entry = null;
                        if (!entryByAddress.TryGetValue(signer.ToString(), out entry))
                        {
                            entry = new AddressBalanceChangeEntry.Entity(txId, signer, blockId);
                            entryByAddress.Add(signer.ToString(), entry);
                        }
                        entry.SpentOutpoints.Add(input.PrevOut);
                    }
                }

                uint i = 0;
                bool hasOpReturn = false;
                foreach (var output in tx.Outputs)
                {
                    if (output.ScriptPubKey.ToRawScript(true)[0] == 0x6a)
                    {
                        hasOpReturn = true;
                        continue;
                    }
                    var receiver = output.ScriptPubKey.GetDestination();
                    if (receiver != null)
                    {
                        AddressBalanceChangeEntry.Entity entry = null;
                        if (!entryByAddress.TryGetValue(receiver.ToString(), out entry))
                        {
                            entry = new AddressBalanceChangeEntry.Entity(txId, receiver, blockId);
                            entry.IsCoinbase = tx.IsCoinBase;
                            entryByAddress.Add(receiver.ToString(), entry);
                        }
                        entry.ReceivedTxOutIndices.Add(i);
                    }
                    i++;
                }
                if (hasOpReturn)
                {
                    foreach (var entity in entryByAddress)
                        entity.Value.HasOpReturn = hasOpReturn;
                }
                return entryByAddress;
            }


            TxDestination _Id;
            public TxDestination Id
            {
                get
                {
                    if (BalanceId != null && _Id == null)
                    {
                        _Id = Helper.DecodeId(BalanceId);
                    }
                    return _Id;
                }
                set
                {
                    _Id = value;
                    BalanceId = Helper.EncodeId(value);
                }
            }

            protected override string CalculatePartitionKey()
            {
                var bytes = Id.ToBytes(true);
                return Helper.GetPartitionKey(12, bytes, bytes.Length - 4, 3);
            }
        }
    }
}
