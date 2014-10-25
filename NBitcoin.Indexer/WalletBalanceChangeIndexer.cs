using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NBitcoin.Indexer
{
    internal class WalletBalanceChangeIndexer : BalanceChangeIndexer<WalletBalanceChangeEntry, WalletBalanceChangeEntry.Entity>
    {
        public IndexerConfiguration Configuration
        {
            get;
            set;
        }
        public IndexerClient Client
        {
            get;
            set;
        }
        public WalletRuleEntryCollection Wallets
        {
            get;
            set;
        }
        public WalletBalanceChangeIndexer(IndexerConfiguration configuration)
        {
            Configuration = configuration;
            Client = Configuration.CreateIndexerClient();
            Configuration.GetWalletRulesTable().CreateIfNotExists();
            Wallets = Client.GetAllWalletRules();
        }
        public override Microsoft.WindowsAzure.Storage.Table.CloudTable GetTable()
        {
            return Configuration.GetWalletBalanceTable();
        }


        protected override WalletBalanceChangeEntry.Entity CreateQueryEntity(string balanceId)
        {
            return new WalletBalanceChangeEntry.Entity(null, balanceId, null);
        }

        protected override WalletBalanceChangeEntry.Entity CreateEntity(DynamicTableEntity tableEntity)
        {
            return new WalletBalanceChangeEntry.Entity(tableEntity, Client);
        }

        protected override WalletBalanceChangeEntry CreateEntry(WalletBalanceChangeEntry.Entity[] entities)
        {
            return new WalletBalanceChangeEntry(entities);
        }

        public override IEnumerable<WalletBalanceChangeEntry.Entity> ExtractFromTransaction(uint256 blockId, Transaction tx, uint256 txId)
        {
            return WalletBalanceChangeEntry.Entity.ExtractFromTransaction(blockId, tx, txId, Wallets).Values;
        }
    }
}
