using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NBitcoin.Indexer
{
    public class IndexerTransactionRepository : ITransactionRepository
    {
        private readonly IndexerServerConfiguration _Configuration;
        public IndexerServerConfiguration Configuration
        {
            get
            {
                return _Configuration;
            }
        }
        public IndexerTransactionRepository(IndexerServerConfiguration config)
        {
            if (config == null)
                throw new ArgumentNullException("config");
            _Configuration = config;
        }
        #region ITransactionRepository Members

        public Transaction Get(uint256 txId)
        {
            var tx = _Configuration.CreateIndexerClient().GetTransaction(txId);
            if (tx == null)
                return null;
            return tx.Transaction;
        }

        public void Put(uint256 txId, Transaction tx)
        {
            _Configuration.CreateIndexer().Index(new TransactionEntry.Entity(tx));
        }

        #endregion
    }
}
