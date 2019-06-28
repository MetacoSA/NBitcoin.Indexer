using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NBitcoin.Indexer
{
    public class OrderBalanceChangeComparer : IComparer<OrderedBalanceChange>
    {
        bool youngToOld;
        OrderBalanceChangeComparer(bool youngToOld)
        {
            this.youngToOld = youngToOld;
        }
        private static OrderBalanceChangeComparer _Youngness = new OrderBalanceChangeComparer(true);
        public static OrderBalanceChangeComparer YoungToOld
        {
            get
            {
                return _Youngness;
            }
        }
        private static readonly OrderBalanceChangeComparer _Oldness = new OrderBalanceChangeComparer(false);
        public static OrderBalanceChangeComparer OldToYoung
        {
            get
            {
                return _Oldness;
            }
        }
        public OrderBalanceChangeComparer Inverse()
        {
            return this == YoungToOld ? OldToYoung : YoungToOld;
        }

        public int Compare(OrderedBalanceChange a, OrderedBalanceChange b)
        {
            var result = CompareCore(a, b);
            if (!youngToOld)
                result = result * -1;
            return result;
        }
        int CompareCore(OrderedBalanceChange a, OrderedBalanceChange b)
        {
            var txIdCompare = a.TransactionId < b.TransactionId ? -1 :
                              a.TransactionId > b.TransactionId ? 1 : 0;
            var seenCompare = (a.SeenUtc < b.SeenUtc ? 1 :
                            a.SeenUtc > b.SeenUtc ? -1 : txIdCompare);
            if (a.BlockId != null && a.Height is int ah)
            {
                // Both confirmed, tie on height then firstSeen
                if (b.BlockId != null && b.Height is int bh)
                {
                    var heightCompare = (ah < bh ? 1 :
                           ah > bh ? -1 : txIdCompare);
                    return ah == bh ?
                           // same height? use firstSeen on firstSeen
                           seenCompare :
                           // else tie on the height
                           heightCompare;
                }
                else
                {
                    return 1;
                }
            }
            else if (b.BlockId != null && b.Height is int bh)
            {
                return -1;
            }
            // Both unconfirmed, tie on firstSeen
            else
            {
                return seenCompare;
            }
        }
    }
}
