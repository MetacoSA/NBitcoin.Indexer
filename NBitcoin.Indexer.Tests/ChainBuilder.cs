using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NBitcoin.Indexer.Tests
{
    class ChainBuilder
    {
        private IndexerTester _Tester;
        Chain _Chain = new Chain(Network.TestNet);

        public ChainBuilder(IndexerTester indexerTester)
        {
            this._Tester = indexerTester;
        }

        Block _Current;

        public Block GetCurrentBlock()
        {
            var b = _Current = _Current ?? CreateNewBlock();
            _Chain.SetTip(b.Header);
            return b;
        }

        public Transaction EmitMoney(IDestination destination, Money amount)
        {
            Transaction transaction = new Transaction();
            transaction.AddInput(new TxIn()
            {
                ScriptSig = new Script(RandomUtils.GetBytes(32)),
            });
            transaction.AddOutput(new TxOut()
            {
                ScriptPubKey = destination.ScriptPubKey,
                Value = amount
            });
            Add(transaction);
            return transaction;
        }

        private void Add(Transaction tx)
        {
            var b = GetCurrentBlock();
            b.Transactions.Add(tx);
            _Tester.Indexer.Index(new TransactionEntry.Entity(null, tx, null));
        }

        private Block CreateNewBlock()
        {
            var b = new Block();
            b.Header.Nonce = RandomUtils.GetUInt32();
            b.Header.HashPrevBlock = _Chain.Tip.HashBlock;
            b.Header.BlockTime = DateTimeOffset.UtcNow;
            return b;
        }

        public Block SubmitBlock()
        {
            var b = GetCurrentBlock();
            _Chain.SetTip(b.Header);
            _Current = null;
            _UnsyncBlocks.Add(b);
            return b;
        }

        List<Block> _UnsyncBlocks = new List<Block>();
        public void SyncIndexer()
        {
            _Tester.Indexer.IndexMainChain(_Chain);
            foreach (var b in _UnsyncBlocks)
            {
                var height = _Chain.GetBlock(b.GetHash()).Height;
                _Tester.Indexer.IndexOrderedBalance(height, b);
            }
            _UnsyncBlocks.Clear();
        }

        public Transaction Emit(Transaction transaction)
        {
            Add(transaction);
            return transaction;
        }
    }
}
