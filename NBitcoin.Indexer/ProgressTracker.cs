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
		private readonly AzureBlockImporter _Importer;
		public AzureBlockImporter Importer
		{
			get
			{
				return _Importer;
			}
		}

		public ImporterConfiguration Configuration
		{
			get
			{
				return _Importer.Configuration;
			}
		}
		public ProgressTracker(AzureBlockImporter importer, DiskBlockPos startPosition)
		{
			_Importer = importer;

			TotalBytes = GetTotalBytes(new DiskBlockPosRange(startPosition));
			ProcessedBytes = 0;
			LastPosition = startPosition;
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
			range = DiskBlockPosRange.All;
			long sum = 0;
			foreach(var file in new DirectoryInfo(Configuration.BlockDirectory).GetFiles().OrderBy(f => f.Name))
			{
				var fileIndex = GetFileIndex(file.Name);
				if(fileIndex < 0)
					continue;
				if(fileIndex > range.End.File)
					continue;
				if(fileIndex == range.End.File)
				{
					sum += Math.Min(file.Length, range.End.Position);
					break;
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
