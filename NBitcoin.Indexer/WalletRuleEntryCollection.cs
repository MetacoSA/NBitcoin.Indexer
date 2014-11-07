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

        MultiValueDictionary<TxDestination, WalletRuleEntry> _EntriesByAddress;
        ILookup<TxDestination, WalletRuleEntry> _EntriesByAddressLookup;


        internal WalletRuleEntryCollection(IEnumerable<WalletRuleEntry> walletRules)
        {
            if (walletRules == null)
                throw new ArgumentNullException("walletRules");

            _WalletRules = new List<WalletRuleEntry>(walletRules);
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
                var rule = entry.Rule as AddressRule;
                if (rule != null)
                    _EntriesByAddress.Add(rule.Id, entry);
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

        public IEnumerable<WalletRuleEntry> GetRulesForAddress(BitcoinAddress address)
        {
            return GetRulesForAddress(address.ID);
        }

        public IEnumerable<WalletRuleEntry> GetRulesForAddress(TxDestination id)
        {
            if (_EntriesByAddress == null)
            {
                _EntriesByAddress = new MultiValueDictionary<TxDestination, WalletRuleEntry>();
                foreach (var entry in this)
                {
                    var rule = entry.Rule as AddressRule;
                    if (rule != null)
                    {
                        _EntriesByAddress.Add(rule.Id, entry);
                    }
                }
                _EntriesByAddressLookup = _EntriesByAddress.AsLookup();
            }
            return _EntriesByAddressLookup[id];
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
