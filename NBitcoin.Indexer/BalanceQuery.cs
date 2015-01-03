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
        internal static BalanceLocator Parse(string str, bool internalFormat)
        {
            var splitted = str.Split(new string[] { "-" }, StringSplitOptions.RemoveEmptyEntries);
            if (splitted.Length == 0)
                throw new FormatException("Invalid BalanceLocator string");
            var height = internalFormat ? Helper.StringToHeight(splitted[0]) : int.Parse(splitted[0]);

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

        internal string ToString(bool internalFormat)
        {
            var height = internalFormat ? Helper.HeightToString(Height) : Height.ToString();
            if (BlockHash != null)
                return height + "-" + BlockHash + "-" + TransactionId;
            else
                return height + "-" + TransactionId;
        }
    }
    public class BalanceQuery
    {
        static uint256 _MaxUInt256;
        static BalanceQuery()
        {
            _MaxUInt256 = new uint256(new byte[32]);
        }
        public BalanceQuery()
        {
            To = new BalanceLocator(0);
            ToIncluded = true;
            From = new BalanceLocator(_MaxUInt256);
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

        public TableQuery CreateEntityQuery(string balanceId)
        {
            var partition = OrderedBalanceChange.GetPartitionKey(balanceId);
            return new TableQuery()
            {
                FilterString =
                TableQuery.CombineFilters(
                                            TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, partition),
                                            TableOperators.And,
                                            TableQuery.CombineFilters(
                                            TableQuery.GenerateFilterCondition("RowKey",
                                                    FromIncluded ? QueryComparisons.GreaterThanOrEqual : QueryComparisons.GreaterThan,
                                                    balanceId + "-" + From.ToString(true)),
                                                TableOperators.And,
                                                TableQuery.GenerateFilterCondition("RowKey",
                                                        ToIncluded ? QueryComparisons.LessThanOrEqual : QueryComparisons.LessThan,
                                                        balanceId + "-" + To.ToString(true))
                                            ))
            };
        }
    }
}
