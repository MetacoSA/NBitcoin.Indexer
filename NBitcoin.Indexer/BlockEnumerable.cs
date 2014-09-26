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

		public IndexerServerConfiguration Configuration
		{
			get
			{
				return _Importer.Configuration;
			}
		}

		public BlockEnumerable(AzureIndexer importer,
							   string checkpointName = null)
		{
            CheckpointInterval = TimeSpan.FromMinutes(10.0);
			this._Importer = importer;
			if(checkpointName == null)
                _ProgressFile = "progress.dat";
			else
			{
                _ProgressFile = checkpointName + "-progress.dat";
			}
            _ProgressFile = Configuration.GetFilePath(_ProgressFile);

			var startPosition = GetCheckpoint();
			var endPosition = new DiskBlockPos((uint)(importer.FromBlk + importer.BlkCount), 0);

			if(importer.FromBlk > startPosition.File ||
				startPosition > endPosition)
			{
				startPosition = new DiskBlockPos((uint)importer.FromBlk, 0);
			}
			else
				IndexerTrace.CheckpointLoaded(startPosition, _ProgressFile);

			IndexerTrace.StartAtPosition(startPosition);
			_Range = new DiskBlockPosRange(startPosition, endPosition);
			_Store = Configuration.CreateBlockStore();
		}

		private string _ProgressFile;
		private ProgressTracker _Progress;
		private AzureIndexer _Importer;
		private DiskBlockPosRange _Range;
		private BlockStore _Store;

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
			if(_Importer.NoSave)
				return;

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
				return (DateTime.Now - _LastSaved) > CheckpointInterval && !_Importer.NoSave;
			}
		}

		#region IEnumerable<StoredBlock> Members

		public IEnumerator<StoredBlock> GetEnumerator()
		{
			_Progress = new ProgressTracker(_Importer, _Range);
			IndexerTrace.ProcessingSize(_Progress.TotalBytes);
			_LastSaved = DateTime.Now;

			var lastLoggedProgress = default(DateTime);
            bool skipFirst = GetCheckpoint() != new DiskBlockPos(0, 0);
            bool first = true;
			foreach(var block in _Store.Enumerate(_Range))
			{
                if (first)
                {
                    first = false;
                    if (skipFirst)
                        continue;
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

        public TimeSpan CheckpointInterval
        {
            get;
            set;
        }
    }
}
