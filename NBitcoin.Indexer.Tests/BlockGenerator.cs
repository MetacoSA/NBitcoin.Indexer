using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NBitcoin.Indexer.Tests
{
	public class BlockGenerator
	{
		BlockStore _Store;
		public BlockGenerator(BlockStore store)
		{
			_Store = store;
		}

		public Block Generate()
		{
			Block block = new Block();
			block.Header.BlockTime = DateTimeOffset.UtcNow;
			block.Header.Nonce = RandomUtils.GetUInt32();
			block.Header.HashPrevBlock = _Chain.Tip.HashBlock;
			_Store.Append(block);
			_Chain.SetTip(block.Header);
			return block;
		}

		private readonly Chain _Chain = new Chain(Network.Main);
		public Chain Chain
		{
			get
			{
				return _Chain;
			}
		}

		public Block Generate(int count)
		{
			Block last = null;
			for(int i = 0 ; i < count ; i++)
			{
				last = Generate();
			}
			return last;
		}
	}
}
