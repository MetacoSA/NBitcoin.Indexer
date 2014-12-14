using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NBitcoin.Indexer
{
    public static class Extensions
    {
        public static IEnumerable<T> Distinct<T, TComparer>(this IEnumerable<T> input, Func<T, TComparer> comparer)
        {
            return input.Distinct(new AnonymousEqualityComparer<T, TComparer>(comparer));
        }

        public static IEnumerable<TBalanceChangeEntry> FetchConfirmedBlocks<TBalanceChangeEntry>(this IEnumerable<TBalanceChangeEntry> entries, ChainBase chain)
            where TBalanceChangeEntry : BalanceChangeEntry
        {
            return entries.Select(e =>
            {
                e.FetchConfirmedBlock(chain);
                return e;
            });
        }

        public static TBalanceChangeEntry FromTransactionId<TBalanceChangeEntry>(this IEnumerable<TBalanceChangeEntry> entries, uint256 txId)
            where TBalanceChangeEntry : BalanceChangeEntry
        {
            return entries.FirstOrDefault(e => e.TransactionId == txId);
        }

        public static SpendableCollection SelectSpentCoins<TBalanceChangeEntry>(this IEnumerable<TBalanceChangeEntry> entries)
           where TBalanceChangeEntry : BalanceChangeEntry
        {
            return SelectSpentCoins(entries, true);
        }

        public static SpendableCollection SelectUnspentCoins<TBalanceChangeEntry>(this IEnumerable<TBalanceChangeEntry> entries)
           where TBalanceChangeEntry : BalanceChangeEntry
        {
            return SelectSpentCoins(entries, false);
        }

        public static IEnumerable<TBalanceChangeEntry> OrderByReceived<TBalanceChangeEntry>(this IEnumerable<TBalanceChangeEntry> entries, ChainBase chain)
            where TBalanceChangeEntry : BalanceChangeEntry
        {
            return entries
                .OrderBy(change => change.Confirmations != 0 ? (long)change.Confirmations :
                                    change.MempoolDate == null ? 0 : 
                                    -change.MempoolDate.Value.Ticks);
        }

        private static SpendableCollection SelectSpentCoins<TBalanceChangeEntry>(IEnumerable<TBalanceChangeEntry> entries, bool spent)
            where TBalanceChangeEntry : BalanceChangeEntry
        {
            SpendableCollection result = new SpendableCollection();
            Dictionary<OutPoint, Spendable> spentCoins = new Dictionary<OutPoint, Spendable>();
            Dictionary<OutPoint, Spendable> receivedCoins = new Dictionary<OutPoint, Spendable>();
            foreach (var entry in entries)
            {
                if (entry.SpentCoins != null)
                {
                    foreach (var c in entry.SpentCoins)
                    {
                        spentCoins.AddOrReplace(c.OutPoint, c);
                    }
                }
                foreach (var c in entry.ReceivedCoins)
                {
                    receivedCoins.AddOrReplace(c.OutPoint, c);
                }
            }
            if (spent)
            {
                result.AddRange(spentCoins.Values.Select(s => s));
            }
            else
            {
                result.AddRange(receivedCoins.Where(r => !spentCoins.ContainsKey(r.Key)).Select(kv => kv.Value));
            }
            return result;
        }

        /// <summary>
        /// Remove unconfirmed for 30minutes
        /// </summary>
        /// <typeparam name="TBalanceChangeEntry"></typeparam>
        /// <param name="entries"></param>
        /// <returns></returns>
        public static IEnumerable<TBalanceChangeEntry> WhereNotExpired<TBalanceChangeEntry>(this IEnumerable<TBalanceChangeEntry> entries)
            where TBalanceChangeEntry : BalanceChangeEntry
        {
            return WhereNotExpired(entries, TimeSpan.FromMinutes(30));
        }

        /// <summary>
        /// Remove unconfirmed entries for expiration
        /// </summary>
        /// <typeparam name="TBalanceChangeEntry"></typeparam>
        /// <param name="entries"></param>
        /// <param name="expiration"></param>
        /// <returns></returns>
        public static IEnumerable<TBalanceChangeEntry> WhereNotExpired<TBalanceChangeEntry>(this IEnumerable<TBalanceChangeEntry> entries, TimeSpan expiration)
            where TBalanceChangeEntry : BalanceChangeEntry
        {
            return entries
                       .Where(e => e.ConfirmedBlock == null &&
                                    e.MempoolDate != null &&
                                    (DateTime.UtcNow - e.MempoolDate.Value) > expiration);
        }

        public static IEnumerable<TBalanceChangeEntry> WhereConfirmed<TBalanceChangeEntry>(this IEnumerable<TBalanceChangeEntry> entries, int minConfirmation = 1)
            where TBalanceChangeEntry : BalanceChangeEntry
        {
            return
                entries
                .Where(e => e.ConfirmedBlock != null && e.Confirmations >= minConfirmation);
        }
    }
}
