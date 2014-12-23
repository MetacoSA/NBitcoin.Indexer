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


        public static CoinCollection SelectSpentCoins(this IEnumerable<OrderedBalanceChange> entries)
        {
            return SelectSpentCoins(entries, true);
        }

        public static CoinCollection SelectUnspentCoins(this IEnumerable<OrderedBalanceChange> entries)
        {
            return SelectSpentCoins(entries, false);
        }

        private static CoinCollection SelectSpentCoins(IEnumerable<OrderedBalanceChange> entries, bool spent)
        {
            CoinCollection result = new CoinCollection();
            Dictionary<OutPoint, Coin> spentCoins = new Dictionary<OutPoint, Coin>();
            Dictionary<OutPoint, Coin> receivedCoins = new Dictionary<OutPoint, Coin>();
            foreach (var entry in entries)
            {
                if (entry.SpentCoins != null)
                {
                    foreach (var c in entry.SpentCoins)
                    {
                        spentCoins.AddOrReplace(c.Outpoint, c);
                    }
                }
                foreach (var c in entry.ReceivedCoins)
                {
                    receivedCoins.AddOrReplace(c.Outpoint, c);
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
        public static IEnumerable<OrderedBalanceChange> WhereNotExpired(this IEnumerable<OrderedBalanceChange> entries)
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
        public static IEnumerable<OrderedBalanceChange> WhereNotExpired(this IEnumerable<OrderedBalanceChange> entries, TimeSpan expiration)
        {
            return entries
                       .Where(e =>
                           e.BlockId != null ||
                           (e.BlockId == null
                                   &&
                                    (DateTime.UtcNow - e.SeenUtc) < expiration));
        }

        public static IEnumerable<OrderedBalanceChange> WhereConfirmed(this IEnumerable<OrderedBalanceChange> entries, ChainBase chain, int minConfirmation = 1)
        {
            return
                entries
                .Where(e => IsMinConf(e, minConfirmation, chain));
        }

        public static BalanceSheet AsBalanceSheet(this IEnumerable<OrderedBalanceChange> entries, ChainBase chain, bool colored = false)
        {
            return new BalanceSheet(entries, chain, colored);
        }

        private static bool IsMinConf(OrderedBalanceChange e, int minConfirmation, ChainBase chain)
        {
            if (e.BlockId == null)
                return minConfirmation == 0;

            var b = chain.GetBlock(e.BlockId);
            if (b == null)
                return false;
            return (chain.Height - b.Height) + 1 >= minConfirmation;
        }
    }
}
