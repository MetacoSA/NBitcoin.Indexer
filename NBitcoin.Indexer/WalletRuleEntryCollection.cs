using NBitcoin.Indexer.Internal;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NBitcoin.Indexer
{
    public class WalletRuleEntryCollection : IEnumerable<WalletRuleEntry>
    {
        List<WalletRuleEntry> _WalletRules;

        MultiValueDictionary<string, WalletRuleEntry> _EntriesByWallet;
        ILookup<string, WalletRuleEntry> _EntriesByWalletLookup;

        MultiValueDictionary<Script, WalletRuleEntry> _EntriesByAddress;
        ILookup<Script, WalletRuleEntry> _EntriesByAddressLookup;


        internal WalletRuleEntryCollection(IEnumerable<WalletRuleEntry> walletRules)
        {
            if (walletRules == null)
                throw new ArgumentNullException("walletRules");

            _WalletRules = new List<WalletRuleEntry>(walletRules);
        }

        public int Count
        {
            get
            {
                return _WalletRules.Count;
            }
        }

        public void Add(WalletRuleEntry entry)
        {
            _WalletRules.Add(entry);
            Added(entry);
        }

        private void Added(WalletRuleEntry entry)
        {
            if (_EntriesByWalletLookup != null)
            {
                _EntriesByWallet.Add(entry.WalletId, entry);
            }
            if (_EntriesByAddress != null)
            {
                var rule = entry.Rule as ScriptRule;
                if (rule != null)
                    _EntriesByAddress.Add(rule.ScriptPubKey, entry);
            }
        }
        public void AddRange(IEnumerable<WalletRuleEntry> entries)
        {
            _WalletRules.AddRange(entries);
            foreach (var e in entries)
                Added(e);
        }

        public IEnumerable<WalletRuleEntry> GetRulesForWallet(string walletName)
        {
            if (_EntriesByWalletLookup == null)
            {
                _EntriesByWallet = new MultiValueDictionary<string, WalletRuleEntry>();
                foreach (var entry in this)
                {
                    _EntriesByWallet.Add(entry.WalletId, entry);
                }
                _EntriesByWalletLookup = _EntriesByWallet.AsLookup();
            }
            return _EntriesByWalletLookup[walletName];
        }


        public IEnumerable<WalletRuleEntry> GetRulesFor(IDestination destination)
        {
            return GetRulesFor(destination.ScriptPubKey);
        }

        public IEnumerable<WalletRuleEntry> GetRulesFor(Script script)
        {
            if (_EntriesByAddress == null)
            {
                _EntriesByAddress = new MultiValueDictionary<Script, WalletRuleEntry>();
                foreach (var entry in this)
                {
                    var rule = entry.Rule as ScriptRule;
                    if (rule != null)
                    {
                        _EntriesByAddress.Add(rule.ScriptPubKey, entry);
                    }
                }
                _EntriesByAddressLookup = _EntriesByAddress.AsLookup();
            }
            return _EntriesByAddressLookup[script];
        }




        #region IEnumerable<WalletRuleEntry> Members

        public IEnumerator<WalletRuleEntry> GetEnumerator()
        {
            return _WalletRules.GetEnumerator();
        }

        #endregion

        #region IEnumerable Members

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        #endregion
    }
}
