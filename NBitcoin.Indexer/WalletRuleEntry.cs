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
            Rule = client.DeserializeRule(Encoding.UTF8.GetString(Encoders.Hex.DecodeData(entity.RowKey)));
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

        public DynamicTableEntity CreateTableEntity()
        {
            DynamicTableEntity entity = new DynamicTableEntity();
            entity.ETag = "*";
            entity.PartitionKey = Encoders.Hex.EncodeData(Encoding.UTF8.GetBytes(WalletId));

            if (Rule != null)
            {
                var txtWriter = new StringWriter();
                JsonTextWriter writer = new JsonTextWriter(txtWriter);
                Rule.WriteJson(writer);
                entity.RowKey = Encoders.Hex.EncodeData(Encoding.UTF8.GetBytes(txtWriter.ToString()));
            }
            return entity;
        }
    }
}
