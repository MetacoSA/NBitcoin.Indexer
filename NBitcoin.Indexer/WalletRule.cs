using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NBitcoin.Indexer
{
    public abstract class WalletRule
    {
        public virtual string TypeName
        {
            get
            {
                return this.GetType().Name;
            }
        }

        public void ReadJson(JsonReader reader, bool skipTypeDeclaration)
        {
            if(!skipTypeDeclaration)
            {
                reader.Read();
                reader.Read();
                reader.Read();
            }

            ReadJsonCore(reader);
        }

        public void WriteJson(JsonWriter writer)
        {
            writer.WriteStartObject();
            writer.WritePropertyName("Type");
            writer.WriteValue(TypeName);
            WriteJsonCore(writer);
            writer.WriteEndObject();
        }

        protected abstract void WriteJsonCore(JsonWriter writer);
        protected abstract void ReadJsonCore(JsonReader reader);
    }
}
