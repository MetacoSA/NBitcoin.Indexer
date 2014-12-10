using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NBitcoin.Indexer
{
    public abstract class WalletRule : ICustomData
    {
        public WalletRule()
        {
            Type = this.GetType().Name;
        }
        public string Type
        {
            get;
            set;
        }

        public ICustomData AttachedData
        {
            get;
            set;
        }

        public string ToString(JsonSerializerSettings serializerSettings)
        {
            return JsonConvert.SerializeObject(this, serializerSettings);
        }
    }
}
