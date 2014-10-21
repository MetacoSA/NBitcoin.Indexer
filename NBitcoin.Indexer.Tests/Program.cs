using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NBitcoin.Indexer.Tests
{
    class Program
    {
        public static void Main(string[] args)
        {
            foreach(var tx in new BlockStore(@"E:\Bitcoin\blocks",Network.Main)
                                         .Enumerate(false)
                                         .SelectMany(i=>i.Item.Transactions))
            {
                AddressBalanceChangeEntry.Entity.ExtractFromTransaction(tx,tx.GetHash());
            }
        }
    }
}
