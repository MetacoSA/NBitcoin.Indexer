using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NBitcoin.Indexer
{
    internal class AddressBalanceChangeIndexer : BalanceChangeIndexer<AddressBalanceChangeEntry,AddressBalanceChangeEntry.Entity>
    {
        public IndexerConfiguration Configuration
        {
            get;
            set;
        }
        public AddressBalanceChangeIndexer(IndexerConfiguration configuration)
        {
            Configuration = configuration;
        }
        public override Microsoft.WindowsAzure.Storage.Table.CloudTable GetTable()
        {
            return Configuration.GetBalanceTable();
        }

        protected override AddressBalanceChangeEntry.Entity CreateQueryEntity(string balanceId)
        {
            return new AddressBalanceChangeEntry.Entity(null, Helper.DecodeScript(balanceId), null);
        }

        protected override AddressBalanceChangeEntry.Entity CreateEntity(DynamicTableEntity tableEntity)
        {
            return new AddressBalanceChangeEntry.Entity(tableEntity);
        }

        protected override AddressBalanceChangeEntry CreateEntry(AddressBalanceChangeEntry.Entity[] entities)
        {
            return new AddressBalanceChangeEntry(entities);
        }

        public override IEnumerable<AddressBalanceChangeEntry.Entity> ExtractFromTransaction(uint256 blockId, Transaction tx, uint256 txId)
        {
            return AddressBalanceChangeEntry.Entity.ExtractFromTransaction(blockId, tx, txId).Values;
        }
    }
}
