using Microsoft.WindowsAzure.Storage.Table;
using NBitcoin.DataEncoders;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NBitcoin.Indexer
{
    public class WalletRuleEntry
    {
        public WalletRuleEntry()
        {

        }
        public WalletRuleEntry(DynamicTableEntity entity, IndexerClient client)
        {
            WalletId = Encoding.UTF8.GetString(Encoders.Hex.DecodeData(entity.PartitionKey));
            Rule = client.Deserialize(Encoding.UTF8.GetString(Encoders.Hex.DecodeData(entity.RowKey)));
        }
        public WalletRuleEntry(string walletId, WalletRule rule)
        {
            WalletId = walletId;
            Rule = rule;
        }
        public string WalletId
        {
            get;
            set;
        }
        public WalletRule Rule
        {
            get;
            set;
        }

        public DynamicTableEntity CreateTableEntity(JsonSerializerSettings serializerSettings)
        {
            DynamicTableEntity entity = new DynamicTableEntity();
            entity.ETag = "*";
            entity.PartitionKey = Encoders.Hex.EncodeData(Encoding.UTF8.GetBytes(WalletId));

            if (Rule != null)
            {
                entity.RowKey = Encoders.Hex.EncodeData(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(Rule, serializerSettings)));
            }
            return entity;
        }
    }
}
