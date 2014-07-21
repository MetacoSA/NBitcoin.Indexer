using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace NBitcoin.Indexer
{
	public class ProgressTracker
	{
		private readonly AzureIndexer _Importer;
		public AzureIndexer Importer
		{
			get
			{
				return _Importer;
			}
		}

		public IndexerServerConfiguration Configuration
		{
			get
			{
				return _Importer.Configuration;
			}
		}
		public ProgressTracker(AzureIndexer importer, DiskBlockPosRange range)
		{
			_Importer = importer;
			TotalBytes = GetTotalBytes(range);
			ProcessedBytes = 0;
			LastPosition = range.Begin;
		}

		public DiskBlockPos LastPosition
		{
			get;
			private set;
		}
		public long TotalBytes
		{
			get;
			private set;
		}
		public long ProcessedBytes
		{
			get;
			private set;
		}
		public double CurrentProgress
		{
			get
			{
				return ((double)ProcessedBytes / (double)TotalBytes) * 100.0;
			}
		}

		private long GetTotalBytes(DiskBlockPosRange range = null)
		{
			if(range == null)
				range = DiskBlockPosRange.All;
			long sum = 0;
			foreach(var file in new DirectoryInfo(Configuration.BlockDirectory).GetFiles().OrderBy(f => f.Name))
			{
				var fileIndex = GetFileIndex(file.Name);
				if(fileIndex < range.Begin.File)
					continue;
				if(fileIndex > range.End.File)
					break;

				if(fileIndex == range.End.File && fileIndex == range.Begin.File)
				{
					var up = Math.Min(file.Length, range.End.Position);
					sum += Math.Max(up - range.Begin.Position, 0);
					continue;
				}
				if(fileIndex == range.End.File)
				{
					sum += Math.Min(file.Length, range.End.Position);
					continue;
				}
				if(fileIndex == range.Begin.File)
				{
					sum += Math.Max(file.Length - range.Begin.Position, 0);
					continue;
				}
				sum += file.Length;
			}
			return sum;
		}
		private int GetFileIndex(string fileName)
		{
			var match = new Regex("blk([0-9]{5,5}).dat").Match(fileName);
			if(!match.Success)
				return -1;
			return int.Parse(match.Groups[1].Value);
		}


		internal void Processing(StoredBlock block)
		{
			if(block.BlockPosition.File == LastPosition.File)
			{
				ProcessedBytes += block.BlockPosition.Position - LastPosition.Position;
			}
			else
			{
				var blkFile = Path.Combine(Configuration.BlockDirectory, "blk" + LastPosition.File.ToString("00000") + ".dat");
				ProcessedBytes += (new FileInfo(blkFile).Length - LastPosition.Position) + block.BlockPosition.Position;
			}
			LastPosition = block.BlockPosition;
		}
	}
}
