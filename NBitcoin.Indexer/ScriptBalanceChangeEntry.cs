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
    public class ScriptBalanceChangeEntry : BalanceChangeEntry
    {
        public ScriptBalanceChangeEntry(params Entity[] entities)
        {
            Init(entities);
            if (entities.Length > 0)
                _ScriptPubKey = entities[0].ScriptPubKey;
        }

        Script _ScriptPubKey;
        public Script ScriptPubKey
        {
            get
            {
                if (_ScriptPubKey == null && BalanceId != null)
                {
                    _ScriptPubKey = Helper.DecodeScript(BalanceId);
                }
                return _ScriptPubKey;
            }
        }

        public new ScriptBalanceChangeEntry FetchConfirmedBlock(ChainBase chain)
        {
            return (ScriptBalanceChangeEntry)base.FetchConfirmedBlock(chain);
        }
        public new class Entity : BalanceChangeEntry.Entity
        {
            public Entity(uint256 txid, Script scriptPubKey, uint256 blockId)
                : base(txid, Helper.EncodeScript(scriptPubKey), blockId)
            {
                _ScriptPubKey = scriptPubKey;
            }
            public Entity(DynamicTableEntity entity)
                : base(entity)
            {
            }

            public static Dictionary<Script, Entity> ExtractFromTransaction(Transaction tx, uint256 txId)
            {
                return ExtractFromTransaction(null, tx, txId);
            }

            static TxNullDataTemplate _OpReturnTemplate = new TxNullDataTemplate();
            public static Dictionary<Script, Entity> ExtractFromTransaction(uint256 blockId, Transaction tx, uint256 txId)
            {
                if (txId == null)
                    txId = tx.GetHash();
                Dictionary<Script, ScriptBalanceChangeEntry.Entity> entryByScriptPubKey = new Dictionary<Script, ScriptBalanceChangeEntry.Entity>();
                foreach (var input in tx.Inputs)
                {
                    if (tx.IsCoinBase)
                        break;
                    var signer = input.ScriptSig.GetSigner();
                    if (signer != null)
                    {
                        ScriptBalanceChangeEntry.Entity entry = null;
                        if (!entryByScriptPubKey.TryGetValue(signer.ScriptPubKey, out entry))
                        {
                            entry = new ScriptBalanceChangeEntry.Entity(txId, signer.ScriptPubKey, blockId);
                            entryByScriptPubKey.Add(signer.ScriptPubKey, entry);
                        }
                        entry.SpentOutpoints.Add(input.PrevOut);
                    }
                }

                uint i = 0;
                bool hasOpReturn = false;
                foreach (var output in tx.Outputs)
                {
                    if (_OpReturnTemplate.CheckScriptPubKey(output.ScriptPubKey))
                    {
                        hasOpReturn = true;
                        i++;
                        continue;
                    }

                    ScriptBalanceChangeEntry.Entity entry = null;
                    if (!entryByScriptPubKey.TryGetValue(output.ScriptPubKey, out entry))
                    {
                        entry = new ScriptBalanceChangeEntry.Entity(txId, output.ScriptPubKey, blockId);
                        entry.IsCoinbase = tx.IsCoinBase;
                        entryByScriptPubKey.Add(output.ScriptPubKey, entry);
                    }
                    entry.ReceivedTxOutIndices.Add(i);

                    i++;
                }
                if (hasOpReturn)
                {
                    foreach (var entity in entryByScriptPubKey)
                        entity.Value.HasOpReturn = hasOpReturn;
                }
                return entryByScriptPubKey;
            }


            Script _ScriptPubKey;
            public Script ScriptPubKey
            {
                get
                {
                    if (BalanceId != null && _ScriptPubKey == null)
                    {
                        _ScriptPubKey = Helper.DecodeScript(BalanceId);
                    }
                    return _ScriptPubKey;
                }
                set
                {
                    _ScriptPubKey = value;
                    BalanceId = Helper.EncodeScript(value);
                }
            }

            protected override string CalculatePartitionKey()
            {
                var bytes = ScriptPubKey.ToBytes(true);
                return Helper.GetPartitionKey(12, bytes, bytes.Length - 4, 3);
            }
        }
    }
}
