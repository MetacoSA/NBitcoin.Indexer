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
        public WalletRule()
        {
        }
        [JsonProperty(DefaultValueHandling=DefaultValueHandling.Ignore)]
        public JToken CustomData
        {
            get;
            set;
        }

        public override string ToString()
        {
            return Helper.Serialize(this);
        }
    }
}
