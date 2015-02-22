using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NBitcoin.Indexer
{
    public class ScriptRule : WalletRule
    {
        public ScriptRule(Script destination, bool isRedeemScript = false)
        {
            if (isRedeemScript)
            {

                ScriptPubKey = destination.Hash.ScriptPubKey;
                RedeemScript = destination;
            }
            else
                ScriptPubKey = destination;
        }
        public ScriptRule(IDestination destination, bool isRedeemScript = false)
            : this(destination.ScriptPubKey, isRedeemScript)
        {
        }
        public ScriptRule()
        {

        }
        [JsonProperty(DefaultValueHandling = DefaultValueHandling.Ignore)]
        public Script ScriptPubKey
        {
            get;
            set;
        }
        [JsonProperty(DefaultValueHandling = DefaultValueHandling.Ignore)]
        public Script RedeemScript
        {
            get;
            set;
        }

        public override string Id
        {
            get
            {
                return ScriptPubKey.Hash.ToString();
            }
        }
    }
}
