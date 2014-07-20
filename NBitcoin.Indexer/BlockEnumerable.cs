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

		public BlockEnumerable(AzureBlockImporter azureBlockImporter,
							   string checkpointName = null)
		{
			this._Importer = azureBlockImporter;
			if(checkpointName == null)
				_ProgressFile = Configuration.ProgressFile;
			else
			{
				var originalName = Path.GetFileName(Configuration.ProgressFile);
				_ProgressFile = checkpointName + "-" + originalName;
			}

			var startPosition = GetCheckpoint();
			IndexerTrace.StartingImportAt(startPosition);
			_Range = new DiskBlockPosRange(startPosition);
			_Store = Configuration.CreateStoreBlock();
		}

		private string _ProgressFile;
		private ProgressTracker _Progress;
		private AzureBlockImporter _Importer;
		private double _LastLoggedProgress;
		private DiskBlockPosRange _Range;
		private BlockStore _Store;
		private TimeSpan saveInterval = TimeSpan.FromMinutes(5.0);
		private Stopwatch _Watch;
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
			IndexerTrace.PositionSaved(Progress.LastPosition);
			if(NeedSave)
			{
				_Watch.Reset();
				_Watch.Start();
				NeedSave = false;
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
			get;
			private set;
		}

		#region IEnumerable<StoredBlock> Members

		public IEnumerator<StoredBlock> GetEnumerator()
		{
			_Progress = new ProgressTracker(_Importer, _Range.Begin);
			_LastLoggedProgress = 0.0;
			_Watch = new Stopwatch();
			_Watch.Start();
			foreach(var block in _Store.Enumerate(_Range))
			{
				_Progress.Processing(block);
				if(_Watch.Elapsed > saveInterval && !NeedSave)
				{
					_Watch.Stop();
					NeedSave = true;
				}
				yield return block;
				IndexerTrace.LogProgress(_Progress, ref _LastLoggedProgress);
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
