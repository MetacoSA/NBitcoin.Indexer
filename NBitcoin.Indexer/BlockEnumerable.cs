using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NBitcoin.Indexer
{
	public class BlockEnumerable : IEnumerable<StoredBlock>
	{

		public ImporterConfiguration Configuration
		{
			get
			{
				return _Importer.Configuration;
			}
		}

		public BlockEnumerable(AzureBlockImporter importer,
							   string checkpointName = null)
		{
			this._Importer = importer;
			if(checkpointName == null)
				_ProgressFile = Configuration.ProgressFile;
			else
			{
				var originalName = Path.GetFileName(Configuration.ProgressFile);
				_ProgressFile = checkpointName + "-" + originalName;
			}

			var startPosition = GetCheckpoint();
			if(importer.FromBlk > startPosition.File)
			{
				startPosition = new DiskBlockPos((uint)importer.FromBlk, 0);
			}
			else
				IndexerTrace.CheckpointLoaded(startPosition, _ProgressFile);

			IndexerTrace.StartAtPosition(startPosition);
			_Range = new DiskBlockPosRange(startPosition);
			_Store = Configuration.CreateStoreBlock();
		}

		private string _ProgressFile;
		private ProgressTracker _Progress;
		private AzureBlockImporter _Importer;
		private DiskBlockPosRange _Range;
		private BlockStore _Store;
		private TimeSpan saveInterval = TimeSpan.FromMinutes(5.0);
		private TimeSpan _LogInterval = TimeSpan.FromSeconds(5.0);
		private DateTime _LastSaved;
		public ProgressTracker Progress
		{
			get
			{
				return _Progress;
			}
		}
		public void SaveCheckpoint()
		{
			File.WriteAllText(_ProgressFile, Progress.LastPosition.ToString());
			IndexerTrace.CheckpointSaved(Progress.LastPosition, _ProgressFile);
			if(NeedSave)
			{
				_LastSaved = DateTime.Now;
			}
		}

		public DiskBlockPos GetCheckpoint()
		{
			try
			{
				return DiskBlockPos.Parse(File.ReadAllText(_ProgressFile));
			}
			catch
			{
			}
			return new DiskBlockPos(0, 0);
		}

		public bool NeedSave
		{
			get
			{
				return (DateTime.Now - _LastSaved) > saveInterval;
			}
		}

		#region IEnumerable<StoredBlock> Members

		public IEnumerator<StoredBlock> GetEnumerator()
		{
			_Progress = new ProgressTracker(_Importer, _Range.Begin);
			_LastSaved = DateTime.Now;

			var lastLoggedProgress = default(DateTime);

			int previousBlk = -1;
			int blkCount = 0;
			foreach(var block in _Store.Enumerate(_Range))
			{
				if(block.BlockPosition.File != previousBlk)
				{
					blkCount++;
					previousBlk = (int)block.BlockPosition.File;
					if(blkCount > _Importer.BlkCount)
						break;
				}
				_Progress.Processing(block);
				yield return block;
				if(DateTime.Now - lastLoggedProgress > _LogInterval)
				{
					lastLoggedProgress = DateTime.Now;
					IndexerTrace.LogProgress(_Progress);
				}
			}
		}

		#endregion

		#region IEnumerable Members

		System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
		{
			return GetEnumerator();
		}

		#endregion
	}
}
