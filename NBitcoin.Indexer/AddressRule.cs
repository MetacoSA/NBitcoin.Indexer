using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NBitcoin.Indexer
{
    public class AddressRule : WalletRule
    {
        public AddressRule(BitcoinAddress address)
        {
            Id = address.Hash;
        }
        public AddressRule(TxDestination id)
        {
            Id = id;
        }
        public AddressRule()
        {

        }

        public TxDestination Id
        {
            get;
            set;
        }

        protected override void WriteJsonCore(JsonWriter writer)
        {
            writer.WritePropertyName("Id");
            writer.WriteValue(Helper.EncodeId(Id));
        }

        protected override void ReadJsonCore(JsonReader reader)
        {
            reader.Read();
            Id = Helper.DecodeId((string)reader.Value);
        }

        public override string ToString()
        {
            var strWriter = new StringWriter();
            JsonTextWriter writer = new JsonTextWriter(strWriter);
            writer.Formatting = Formatting.Indented;
            WriteJson(writer);
            return strWriter.ToString();
        }
    }
}
