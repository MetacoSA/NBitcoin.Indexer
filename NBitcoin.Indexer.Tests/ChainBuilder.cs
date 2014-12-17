using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NBitcoin.Indexer.Tests
{
    public class ChainBuilder
    {
        private IndexerTester _Tester;
        Chain _Chain = new Chain(Network.TestNet);

        public Chain Chain
        {
            get
            {
                return _Chain;
            }
        }
        public ChainBuilder(IndexerTester indexerTester)
        {
            this._Tester = indexerTester;
            var genesis = indexerTester.Indexer.Configuration.Network.GetGenesis();
            _Blocks.Add(genesis.GetHash(), genesis);
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
            _Blocks.Add(b.Header.GetHash(), b);
            _Mempool.Clear();
            return b;
        }

        List<Block> _UnsyncBlocks = new List<Block>();
        public void SyncIndexer()
        {
            _Tester.Indexer.IndexChain(_Chain);
            var walletRules = _Tester.Client.GetAllWalletRules();
            foreach (var b in _UnsyncBlocks)
            {
                var height = _Chain.GetBlock(b.GetHash()).Height;
                _Tester.Indexer.IndexOrderedBalance(height, b);
                if (walletRules.Count() != 0)
                {
                    _Tester.Indexer.IndexWalletOrderedBalance(height, b, walletRules);
                }
            }
            _UnsyncBlocks.Clear();
        }

        public Transaction Emit(Transaction transaction)
        {
            Add(transaction);
            _Mempool.Add(transaction.GetHash(), transaction);
            return transaction;
        }

        public Block Generate(int count = 1)
        {
            Block b = null;
            for (int i = 0 ; i < count ; i++)
                b = SubmitBlock();
            return b;
        }


        public void Emit(IEnumerable<Transaction> transactions)
        {
            foreach (var tx in transactions)
                Emit(tx);
        }

        private readonly Dictionary<uint256,Block> _Blocks = new Dictionary<uint256,Block>();
        public Dictionary<uint256,Block> Blocks
        {
            get
            {
                return _Blocks;
            }
        }

        private readonly Dictionary<uint256,Transaction> _Mempool = new Dictionary<uint256,Transaction>();
        public Dictionary<uint256,Transaction> Mempool
        {
            get
            {
                return _Mempool;
            }
        }

        public void Load(string blockFolder)
        {
            var store = new BlockStore(blockFolder, this._Tester.Client.Configuration.Network);
            foreach (var block in store.Enumerate(false))
            {
                SubmitBlock(block.Item);
            }
        }

        public void SubmitBlock(Block block)
        {
            if (!Blocks.ContainsKey(block.GetHash()))
            {
                _Current = block;
                SubmitBlock();
            }
        }

    }
}
