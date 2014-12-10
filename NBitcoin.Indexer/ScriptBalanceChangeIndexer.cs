using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NBitcoin.Indexer
{
    internal class ScriptBalanceChangeIndexer : BalanceChangeIndexer<ScriptBalanceChangeEntry,ScriptBalanceChangeEntry.Entity>
    {
        public IndexerConfiguration Configuration
        {
            get;
            set;
        }
        public ScriptBalanceChangeIndexer(IndexerConfiguration configuration)
        {
            Configuration = configuration;
        }
        public override Microsoft.WindowsAzure.Storage.Table.CloudTable GetTable()
        {
            return Configuration.GetBalanceTable();
        }

        protected override ScriptBalanceChangeEntry.Entity CreateQueryEntity(string balanceId)
        {
            return new ScriptBalanceChangeEntry.Entity(null, Helper.DecodeScript(balanceId), null);
        }

        protected override ScriptBalanceChangeEntry.Entity CreateEntity(DynamicTableEntity tableEntity)
        {
            return new ScriptBalanceChangeEntry.Entity(tableEntity);
        }

        protected override ScriptBalanceChangeEntry CreateEntry(ScriptBalanceChangeEntry.Entity[] entities)
        {
            return new ScriptBalanceChangeEntry(entities);
        }

        public override IEnumerable<ScriptBalanceChangeEntry.Entity> ExtractFromTransaction(uint256 blockId, Transaction tx, uint256 txId)
        {
            return ScriptBalanceChangeEntry.Entity.ExtractFromTransaction(blockId, tx, txId).Values;
        }
    }
}
