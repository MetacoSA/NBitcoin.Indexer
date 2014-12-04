using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NBitcoin.Indexer
{
	public static class ChainChangeEntryExtensions
	{
		public static void UpdateChain(this IEnumerable<ChainChangeEntry> entries, ChainBase chain)
		{
			Stack<ChainChangeEntry> toApply = new Stack<ChainChangeEntry>();
			foreach(var entry in entries)
			{
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
	public class ChainChangeEntry
	{
		public class Entity : TableEntity
		{
			public Entity()
			{

			}
			public Entity(byte[] header, byte[] blockId, int height)
			{
				Header = header;
				PartitionKey = GetPartitionKey(height);
				RowKey = HeightToString(height);
				BlockId = blockId;
			}

			public static string GetPartitionKey(int height)
			{
				return HeightToString(height / 100);
			}
			public byte[] BlockId
			{
				get;
				set;
			}
			public byte[] Header
			{
				get;
				set;
			}
			public ChainChangeEntry ToObject()
			{
				ChainChangeEntry entry = new ChainChangeEntry();
				entry.Height = StringToHeight(RowKey);
				entry.BlockId = new uint256(BlockId);
				entry.Header = new BlockHeader();
				entry.Header.FromBytes(Header);
				return entry;
			}

			static string format = new string(Enumerable.Range(0, int.MaxValue.ToString().Length).Select(c => '0').ToArray());

			//Convert '012' to '987'
			private static string HeightToString(int height)
			{
				var input = height.ToString(format);
				char[] result = new char[format.Length];
				for(int i = 0 ; i < result.Length ; i++)
				{
					var index = Array.IndexOf(Digit, input[i]);
					result[i] = InvertDigit[index];
				}
				return new string(result);
			}

			//Convert '987' to '012'
			private int StringToHeight(string rowkey)
			{
				char[] result = new char[format.Length];
				for(int i = 0 ; i < result.Length ; i++)
				{
					var index = Array.IndexOf(InvertDigit, rowkey[i]);
					result[i] = Digit[index];
				}
				return int.Parse(new string(result));
			}

			static char[] Digit = Enumerable.Range(0, 10).Select(c => c.ToString()[0]).ToArray();
			static char[] InvertDigit = Enumerable.Range(0, 10).Reverse().Select(c => c.ToString()[0]).ToArray();


		}

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

		internal Entity ToEntity()
		{
			return new Entity(Header.ToBytes(), BlockId.ToBytes(), Height);
		}

		public override string ToString()
		{
			return Height + "-" + BlockId;
		}
	}
}
