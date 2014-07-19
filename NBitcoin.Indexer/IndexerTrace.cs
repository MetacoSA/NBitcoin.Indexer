using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NBitcoin.Indexer
{
	class IndexerTrace
	{
		static TraceSource _Trace = new TraceSource("NBitcoin.Indexer");
		internal static void ErrorWhileImportingBlockToAzure(uint256 id, Exception ex)
		{
			_Trace.TraceEvent(TraceEventType.Error, 0, "Error while importing " + id + " in azure blob : " + Utils.ExceptionToString(ex));
		}


		internal static void BlockAlreadyUploaded()
		{
			_Trace.TraceInformation("Block already uploaded");
		}

		internal static void BlockUploaded(TimeSpan time, int bytes)
		{
			if(time.TotalSeconds == 0.0)
				time = TimeSpan.FromMilliseconds(10);
			double speed = ((double)bytes / 1024.0) / time.TotalSeconds;
			_Trace.TraceEvent(TraceEventType.Verbose, 0, "Block uploaded successfully (" + speed.ToString("0.00") + " KB/S)");
		}

		internal static TraceCorrelation NewCorrelation(string activityName)
		{
			return new TraceCorrelation(_Trace, activityName);
		}

		internal static void StartingImportAt(DiskBlockPos lastPosition)
		{
			_Trace.TraceInformation("Starting import at position " + lastPosition);
		}

		internal static void PositionSaved(DiskBlockPos diskBlockPos)
		{
			_Trace.TraceInformation("New starting import position : " + diskBlockPos.ToString());
		}

		internal static void BlockCount(int blockCount, bool verbose)
		{
			_Trace.TraceEvent((!verbose) ? TraceEventType.Information : TraceEventType.Verbose, 0, "Block count : " + blockCount);
		}

		internal static void TxCount(int txCount, bool verbose)
		{
			_Trace.TraceEvent((!verbose) ? TraceEventType.Information : TraceEventType.Verbose, 0, "Transaction count : " + txCount);
		}

		internal static void ErrorWhileImportingTxToAzure(IndexedTransaction[] transactions, Exception ex)
		{
			StringBuilder builder = new StringBuilder();
			int i = 0;
			foreach(var tx in transactions)
			{
				builder.AppendLine("[ " + i + "] " + tx.PartitionKey + " " + tx.RowKey);
				i++;
			}
			_Trace.TraceEvent(TraceEventType.Error, 0, "Error while importing transactions (len:" + transactions.Length + ") : " + Utils.ExceptionToString(ex) + "\r\n" + builder.ToString());
		}

		internal static void RetryWorked()
		{
			_Trace.TraceInformation("Retry worked");
		}

		internal static void LogProgress(ProgressTracker progress, ref double lastLoggedProgress)
		{
			var verbose = progress.CurrentProgress - lastLoggedProgress < 0.1;
			if(!verbose)
				lastLoggedProgress = progress.CurrentProgress;

			var info = "Progress : " + progress.CurrentProgress.ToString(verbose ? "0.0000" : "0.00");
			_Trace.TraceEvent(verbose ? TraceEventType.Verbose : TraceEventType.Information, 0, info);
		}
	}
}
