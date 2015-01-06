using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NBitcoin.Indexer
{
    public class BalanceLocator
    {
        public BalanceLocator(OrderedBalanceChange change)
            : this(change.Height, change.BlockId, change.TransactionId)
        {

        }
        public BalanceLocator(int height, uint256 blockId = null, uint256 transactionId = null)
        {
            Height = height;
            BlockHash = blockId;
            TransactionId = transactionId;
        }

        public BalanceLocator(uint256 unconfTransactionId)
        {
            TransactionId = unconfTransactionId;
            Height = int.MaxValue;
        }

        public int Height
        {
            get;
            set;
        }


        public uint256 BlockHash
        {
            get;
            set;
        }

        public uint256 TransactionId
        {
            get;
            set;
        }


        public static BalanceLocator Parse(string str)
        {
            return Parse(str, false);
        }
        internal static BalanceLocator Parse(string str, bool queryFormat)
        {
            var splitted = str.Split(new string[] { "-" }, StringSplitOptions.RemoveEmptyEntries);
            if (splitted.Length == 0)
                throw new FormatException("Invalid BalanceLocator string");
            var height = queryFormat ? Helper.StringToHeight(splitted[0]) : int.Parse(splitted[0]);

            uint256 transactionId = null;
            uint256 blockId = null;
            if (height == int.MaxValue)
            {
                if (splitted.Length >= 2)
                    transactionId = new uint256(splitted[1]);
            }
            else
            {
                if (splitted.Length >= 2)
                    blockId = new uint256(splitted[1]);
                if (splitted.Length >= 3)
                    transactionId = new uint256(splitted[2]);
            }
            return new BalanceLocator(height, blockId, transactionId);
        }

        public override string ToString()
        {
            return ToString(false);
        }

        public string ToString(bool queryFormat)
        {
            var height = queryFormat ? Helper.HeightToString(Height) : Height.ToString();
            if (BlockHash != null)
                return height + "-" + BlockHash + "-" + TransactionId;
            else
                return height + "-" + TransactionId;
        }
    }

    public class BalanceQuery
    {
        static uint256 _MinUInt256;
        static uint256 _MaxUInt256;
        static BalanceQuery()
        {
            _MinUInt256 = new uint256(new byte[32]);
            var b = new byte[32];
            for (int i = 0 ; i < b.Length ; i++)
            {
                b[i] = 0xFF;
            }
            _MaxUInt256 = new uint256(b);
        }
        public BalanceQuery()
        {
            From = new BalanceLocator(int.MaxValue);
            To = new BalanceLocator(0);
            ToIncluded = true;
            FromIncluded = true;
        }
        public BalanceLocator To
        {
            get;
            set;
        }
        public bool ToIncluded
        {
            get;
            set;
        }

        public BalanceLocator From
        {
            get;
            set;
        }

        public bool FromIncluded
        {
            get;
            set;
        }

        public IEnumerable<int> PageSizes
        {
            get;
            set;
        }


        public TableQuery CreateTableQuery(string balanceId)
        {
            var partition = OrderedBalanceChange.GetPartitionKey(balanceId);
            return CreateTableQuery(partition, balanceId);
        }

        public TableQuery CreateTableQuery(string partitionId, string scope)
        {
            var from = From ?? new BalanceLocator(int.MaxValue);
            var to = To ?? new BalanceLocator(0);
            var toIncluded = ToIncluded;
            var fromIncluded = FromIncluded;

            //Fix automatically if wrong order
            if (from.Height < to.Height)
            {
                var temp = to;
                var temp2 = toIncluded;
                to = from;
                toIncluded = FromIncluded;
                from = temp;
                fromIncluded = temp2;
            }
            ////

            //Complete the balance locator if partial
            if (from.TransactionId == null)
                from = new BalanceLocator(from.Height, transactionId: new uint256(fromIncluded ? _MinUInt256 : _MaxUInt256));

            if (from.BlockHash == null)
                from = new BalanceLocator(from.Height, from.TransactionId, new uint256(fromIncluded ? _MinUInt256 : _MaxUInt256));

            if (to.TransactionId == null)
                to = new BalanceLocator(to.Height, transactionId: new uint256(toIncluded ? _MaxUInt256 : _MinUInt256));

            if (to.BlockHash == null)
                to = new BalanceLocator(to.Height, to.TransactionId, new uint256(toIncluded ? _MaxUInt256 : _MinUInt256));
            /////

            return new TableQuery()
            {
                FilterString =
                TableQuery.CombineFilters(
                                            TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, partitionId),
                                            TableOperators.And,
                                            TableQuery.CombineFilters(
                                            TableQuery.GenerateFilterCondition("RowKey",
                                                    fromIncluded ? QueryComparisons.GreaterThanOrEqual : QueryComparisons.GreaterThan,
                                                    scope + "-" + from.ToString(true)),
                                                TableOperators.And,
                                                TableQuery.GenerateFilterCondition("RowKey",
                                                        toIncluded ? QueryComparisons.LessThanOrEqual : QueryComparisons.LessThan,
                                                        scope + "-" + to.ToString(true))
                                            ))
            };
        }

    }
}
