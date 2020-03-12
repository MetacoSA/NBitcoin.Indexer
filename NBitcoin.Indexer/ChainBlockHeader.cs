using Microsoft.WindowsAzure.Storage.Table;
using System;
using Dasync.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace NBitcoin.Indexer
{
	public static class ChainChangeEntryExtensions
	{
        public static void UpdateChain(this IEnumerable<ChainBlockHeader> entries, ChainBase chain)
        {
            var entriesAsync = new AsyncEnumerable<ChainBlockHeader>(async yield =>
            {
                foreach (var entry in entries)
                {
                    await yield.ReturnAsync(entry);
                }
            });
            UpdateChain(entriesAsync, chain, default(CancellationToken)).GetAwaiter().GetResult();
        }
        public static async Task UpdateChain(this IAsyncEnumerable<ChainBlockHeader> entries, ChainBase chain, CancellationToken cancellation = default(CancellationToken))
		{
            var enumerator = entries.GetAsyncEnumerator(cancellation);
			Stack<ChainBlockHeader> toApply = new Stack<ChainBlockHeader>();
			while(await enumerator.MoveNextAsync().ConfigureAwait(false))
			{
                var entry = enumerator.Current;
				var prev = chain.GetBlock(entry.Header.HashPrevBlock);
				if(prev == null)
					toApply.Push(entry);
				else
				{
					toApply.Push(entry);
					break;
				}
			}
			while(toApply.Count > 0)
			{
				var newTip = toApply.Pop();

				var chained = new ChainedBlock(newTip.Header, newTip.BlockId, chain.GetBlock(newTip.Header.HashPrevBlock));
				chain.SetTip(chained);
			}
		}
	}
	public class ChainBlockHeader
	{
		public uint256 BlockId
		{
			get;
			set;
		}

		public int Height
		{
			get;
			set;
		}
		public BlockHeader Header
		{
			get;
			set;
		}

		public override string ToString()
		{
			return Height + "-" + BlockId;
		}
	}
}
