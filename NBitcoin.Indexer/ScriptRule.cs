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
        public ScriptRule(Script script)
        {
            Script = script;
        }
        public ScriptRule(IDestination destination)
        {
            Script = destination.ScriptPubKey;
        }
        public ScriptRule()
        {

        }

        public Script Script
        {
            get;
            set;
        }
    }
}
