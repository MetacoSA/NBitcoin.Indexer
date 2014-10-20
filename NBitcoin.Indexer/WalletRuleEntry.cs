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
        public WalletRuleEntry(DynamicTableEntity entity, Dictionary<string, Func<WalletRule>> rulenameMapping)
        {
            WalletId = Encoding.UTF8.GetString(Encoders.Hex.DecodeData(entity.PartitionKey));
            JsonTextReader reader = new JsonTextReader(new StringReader(Encoding.UTF8.GetString(Encoders.Hex.DecodeData(entity.RowKey))));
            reader.Read();
            reader.Read();
            reader.Read();
            var type = (string)reader.Value;
            if (!rulenameMapping.ContainsKey(type))
                throw new InvalidOperationException("Type " + rulenameMapping + " not registered with AzureIndexer.AddWalletRuleTypeConverter");
            Rule = rulenameMapping[type]();
            reader.Read();
            Rule.ReadJson(reader, true);
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
