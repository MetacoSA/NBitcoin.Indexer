using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NBitcoin.Indexer
{
    public class IndexerTransactionRepository : ITransactionRepository
    {
        private readonly IndexerConfiguration _Configuration;
        public IndexerConfiguration Configuration
        {
            get
            {
                return _Configuration;
            }
        }
        public IndexerTransactionRepository(IndexerConfiguration config)
        {
            if (config == null)
                throw new ArgumentNullException("config");
            _Configuration = config;
        }
        #region ITransactionRepository Members

        public Transaction Get(uint256 txId)
        {
            var tx = _Configuration.CreateIndexerClient().GetTransactionAsync(false, txId).Result;
            if (tx == null)
                return null;
            return tx.Transaction;
        }

        public void Put(uint256 txId, Transaction tx)
        {
            _Configuration.CreateIndexer().Index(new TransactionEntry.Entity(txId, tx, null));
        }

        #endregion
    }
}
