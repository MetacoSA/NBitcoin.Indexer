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

        public Script ScriptPubKey
        {
            get;
            set;
        }

        public Script RedeemScript
        {
            get;
            set;
        }
    }
}
