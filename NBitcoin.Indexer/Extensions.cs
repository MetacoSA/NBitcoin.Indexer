using Microsoft.Data.OData;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Table;
using Microsoft.WindowsAzure.Storage.Table.Protocol;
using NBitcoin.Crypto;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Async;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace NBitcoin.Indexer
{
    public static class Extensions
    {
        class OrderBalanceChangeComparer : IComparer<OrderedBalanceChange>
        {

            private static readonly OrderBalanceChangeComparer _Instance = new OrderBalanceChangeComparer();
            public static OrderBalanceChangeComparer Instance
            {
                get
                {
                    return _Instance;
                }
            }
            public int Compare(OrderedBalanceChange a, OrderedBalanceChange b)
            {
                var txIdCompare = a.TransactionId < b.TransactionId ? -1 :
                                  a.TransactionId > b.TransactionId ? 1 : 0;
                var seenCompare = (a.SeenUtc < b.SeenUtc ? -1 :
                                a.SeenUtc > b.SeenUtc ? 1 : txIdCompare);
                if (a.BlockId != null && a.Height is int ah)
                {
                    // Both confirmed, tie on height then firstSeen
                    if (b.BlockId != null && b.Height is int bh)
                    {
                        var heightCompare = (ah < bh ? -1 :
                               ah > bh ? 1 : txIdCompare);
                        return ah == bh ?
                               // same height? use firstSeen on firstSeen
                               seenCompare :
                               // else tie on the height
                               heightCompare;
                    }
                    else
                    {
                        return -1;
                    }
                }
                else if (b.BlockId != null && b.Height is int bh)
                {
                    return 1;
                }
                // Both unconfirmed, tie on firstSeen
                else
                {
                    return seenCompare;
                }
            }
        }

        public static async Task<T[]> ToArrayAsync<T>(this Task<ICollection<T>> transactions)
        {
            return (await transactions).ToArray();
        }

        public static List<OrderedBalanceChange> TopologicalSort(this ICollection<OrderedBalanceChange> transactions)
        {
            return transactions.TopologicalSort(
                dependsOn: t => t.SpentCoins.Select(o => o.Outpoint.Hash),
                getKey: t => t.TransactionId,
                getValue: t => t,
                solveTies: OrderBalanceChangeComparer.Instance);
        }
        public static List<T> TopologicalSort<T>(this ICollection<T> nodes, Func<T, IEnumerable<T>> dependsOn)
        {
            return nodes.TopologicalSort(dependsOn, k => k, k => k);
        }

        public static List<T> TopologicalSort<T, TDepend>(this ICollection<T> nodes, Func<T, IEnumerable<TDepend>> dependsOn, Func<T, TDepend> getKey)
        {
            return nodes.TopologicalSort(dependsOn, getKey, o => o);
        }

        public static List<TValue> TopologicalSort<T, TDepend, TValue>(this ICollection<T> nodes,
                                                Func<T, IEnumerable<TDepend>> dependsOn,
                                                Func<T, TDepend> getKey,
                                                Func<T, TValue> getValue,
                                                IComparer<T> solveTies = null)
        {
            if (nodes.Count == 0)
                return new List<TValue>();
            if (getKey == null)
                throw new ArgumentNullException(nameof(getKey));
            if (getValue == null)
                throw new ArgumentNullException(nameof(getValue));
            solveTies = solveTies ?? Comparer<T>.Default;
            List<TValue> result = new List<TValue>(nodes.Count);
            HashSet<TDepend> allKeys = new HashSet<TDepend>();
            var noDependencies = new SortedDictionary<T, HashSet<TDepend>>(solveTies);

            foreach (var node in nodes)
                allKeys.Add(getKey(node));
            var dependenciesByValues = nodes.ToDictionary(node => node,
                                           node => new HashSet<TDepend>(dependsOn(node).Where(n => allKeys.Contains(n))));
            foreach (var e in dependenciesByValues.Where(x => x.Value.Count == 0))
            {
                noDependencies.Add(e.Key, e.Value);
            }
            if (noDependencies.Count == 0)
            {
                throw new InvalidOperationException("Impossible to topologically sort a cyclic graph");
            }
            while (noDependencies.Count > 0)
            {
                var nodep = noDependencies.First();
                noDependencies.Remove(nodep.Key);
                dependenciesByValues.Remove(nodep.Key);

                var elemKey = getKey(nodep.Key);
                result.Add(getValue(nodep.Key));
                foreach (var selem in dependenciesByValues)
                {
                    if (selem.Value.Remove(elemKey) && selem.Value.Count == 0)
                        noDependencies.Add(selem.Key, selem.Value);
                }
            }
            if (dependenciesByValues.Count != 0)
            {
                throw new InvalidOperationException("Impossible to topologically sort a cyclic graph");
            }
            return result;
        }
        public static IAsyncEnumerable<List<T>> Partition<T>(this IAsyncEnumerable<T> source, int max, CancellationToken cancellationToken)
        {
            return Partition(source, () => max, cancellationToken);
        }
        public static IAsyncEnumerable<List<T>> Partition<T>(this IAsyncEnumerable<T> source, Func<int> max, CancellationToken cancellationToken)
        {
            return new AsyncEnumerable<List<T>>(async yield =>
            {
                var partitionSize = max();
                List<T> toReturn = new List<T>(partitionSize);
                var enumerator = await source.GetAsyncEnumeratorAsync(cancellationToken);
                while (await enumerator.MoveNextAsync(cancellationToken))
                {
                    var item = enumerator.Current;
                    toReturn.Add(item);
                    if (toReturn.Count == partitionSize)
                    {
                        await yield.ReturnAsync(toReturn);
                        partitionSize = max();
                        toReturn = new List<T>(partitionSize);
                    }
                }
                if (toReturn.Any())
                {
                    await yield.ReturnAsync(toReturn);
                }
            });
        }

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
            Dictionary<OutPoint, ICoin> spentCoins = new Dictionary<OutPoint, ICoin>();
            Dictionary<OutPoint, ICoin> receivedCoins = new Dictionary<OutPoint, ICoin>();
            foreach(var entry in entries)
            {
                if(entry.SpentCoins != null)
                {
                    foreach(var c in entry.SpentCoins)
                    {
                        spentCoins.AddOrReplace(c.Outpoint, c);
                    }
                }
                foreach(var c in entry.ReceivedCoins)
                {
                    receivedCoins.AddOrReplace(c.Outpoint, c);
                }
            }
            if(spent)
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
        /// <param name="entries"></param>
        /// <returns></returns>
        public static IEnumerable<OrderedBalanceChange> WhereNotExpired(this IEnumerable<OrderedBalanceChange> entries)
        {
            return WhereNotExpired(entries, TimeSpan.FromMinutes(30));
        }

        /// <summary>
        /// Remove unconfirmed entries for expiration
        /// </summary>
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

        public static async Task<BalanceSheet> AsBalanceSheet(this Task<ICollection<OrderedBalanceChange>> entries, ChainBase chain)
        {
            return new BalanceSheet(await entries.ToArrayAsync(), chain);
        }
        public static BalanceSheet AsBalanceSheet(this IEnumerable<OrderedBalanceChange> entries, ChainBase chain, CancellationToken cancellationToken = default(CancellationToken))
        {
            return new BalanceSheet(entries, chain);
        }

        private static bool IsMinConf(OrderedBalanceChange e, int minConfirmation, ChainBase chain)
        {
            if(e.BlockId == null)
                return minConfirmation == 0;

            var b = chain.GetBlock(e.BlockId);
            if(b == null)
                return false;
            return (chain.Height - b.Height) + 1 >= minConfirmation;
        }

        public static void MakeFat(this DynamicTableEntity entity, int size)
        {
            entity.Properties.Clear();
            entity.Properties.Add("fat", new EntityProperty(size));
        }
        public static bool IsFat(this DynamicTableEntity entity)
        {
            return entity.Properties.Any(p => p.Key.Equals("fat", StringComparison.OrdinalIgnoreCase) &&
                                              p.Value.PropertyType == EdmType.Int32);
        }
        public static string GetFatBlobName(this ITableEntity entity)
        {
            return "unk" + Hashes.Hash256(Encoding.UTF8.GetBytes(entity.PartitionKey + entity.RowKey)).ToString();
        }

		public static byte[] Serialize(this ITableEntity entity)
        {
            MemoryStream ms = new MemoryStream();
            using(var messageWriter = new ODataMessageWriter(new Message(ms), new ODataMessageWriterSettings()))
            {
                // Create an entry writer to write a top-level entry to the message.
                ODataWriter entryWriter = messageWriter.CreateODataEntryWriter();
				TableOperationHttpWebRequestFactory.WriteOdataEntity(entity, TableOperationType.Insert, null, entryWriter, null, true);
                return ms.ToArray();
            }
        }

		public static void Deserialize(this ITableEntity entity, byte[] value)
        {
            MemoryStream ms = new MemoryStream(value);
            using(ODataMessageReader messageReader = new ODataMessageReader(new Message(ms), new ODataMessageReaderSettings()
            {
                MessageQuotas = new ODataMessageQuotas()
                {
                    MaxReceivedMessageSize = 20 * 1024 * 1024
                }
            }))
            {
                ODataReader reader = messageReader.CreateODataEntryReader();
                reader.Read();
				TableOperationHttpWebRequestFactory.ReadAndUpdateTableEntity(entity, (ODataEntry)reader.Item, null);
            }
        }

        internal class Message : IODataResponseMessage
        {
            private readonly Stream stream;
            private readonly Dictionary<string, string> headers = new Dictionary<string, string>();

            public Message(Stream stream)
            {
                this.stream = stream;
                SetHeader("Content-Type", "application/atom+xml");
            }

            public string GetHeader(string headerName)
            {
                string value;
                headers.TryGetValue(headerName, out value);
                return value;
            }

            public void SetHeader(string headerName, string headerValue)
            {
                this.headers.Add(headerName, headerValue);
            }

            public Stream GetStream()
            {
                return this.stream;
            }

            public IEnumerable<KeyValuePair<string, string>> Headers
            {
                get
                {
                    return this.headers;
                }
            }

            public int StatusCode
            {
                get;
                set;
            }
        }
    }
}
