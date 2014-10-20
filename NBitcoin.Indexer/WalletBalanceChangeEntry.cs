using Microsoft.WindowsAzure.Storage.Table;
using NBitcoin.Indexer.Internal;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NBitcoin.Indexer
{
    public class WalletBalanceChangeEntry : BalanceChangeEntry
    {
        public WalletBalanceChangeEntry(params Entity[] entities)
            : base(entities.OfType<BalanceChangeEntry.Entity>().ToArray())
        {
           
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
            public Entity(uint256 txid, string walletId, uint256 blockId)
                : base(txid, walletId, blockId)
            {
            }
            public Entity(DynamicTableEntity entity)
                : base(entity)
            {
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
                if(bytes.Length >= 3)
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
                                                                            IndexerClient indexerClient)
            {
                var walletsByAddress = new MultiValueDictionary<string, WalletRuleEntry>();
                foreach(var walletRule in indexerClient.GetAllWalletRules())
                {
                    if (walletRule.Rule is AddressRule)
                    {
                        walletsByAddress.Add(((AddressRule)walletRule.Rule).Id.ToString(), walletRule);
                    }
                }

                Dictionary<string, Entity> entitiesByWallet = new Dictionary<string, Entity>();
                var entryByAddress = AddressEntry.Entity.ExtractFromTransaction(blockId, tx, txId);
                foreach (var entryAddress in entryByAddress)
                {
                    foreach (var walletRuleEntry in walletsByAddress.AsLookup()[entryAddress.Key])
                    {
                        Entity walletEntity = null;
                        if(!entitiesByWallet.TryGetValue(walletRuleEntry.WalletId,out walletEntity))
                        {
                            walletEntity = new Entity(txId, walletRuleEntry.WalletId, blockId);
                            entitiesByWallet.Add(walletRuleEntry.WalletId, walletEntity);
                        }
                        walletEntity.ReceivedTxOutIndices.AddRange(entryAddress.Value.ReceivedTxOutIndices);
                        walletEntity.SpentOutpoints.AddRange(entryAddress.Value.SpentOutpoints);
                    }
                }
                foreach (var wallet in entitiesByWallet)
                {
                    wallet.Value.SpentOutpoints.Distinct();
                    wallet.Value.SpentOutpoints.Distinct();
                }
                return entitiesByWallet;
            }
        }
    }
}
