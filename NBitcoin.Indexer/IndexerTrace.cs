using Microsoft.WindowsAzure.Storage.Table;
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
			_Trace.TraceEvent(TraceEventType.Verbose, 0, "Block already uploaded");
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

		internal static void CheckpointLoaded(DiskBlockPos lastPosition, string file)
		{
			_Trace.TraceInformation("Checkpoint loaded " + "(" + file + ")");
		}

		internal static void CheckpointSaved(DiskBlockPos diskBlockPos, string file)
		{
			_Trace.TraceInformation("New checkpoint : " + diskBlockPos.ToString() + " (" + file + ")");
		}


		internal static void ErrorWhileImportingEntitiesToAzure(TableEntity[] entities, Exception ex)
		{
			StringBuilder builder = new StringBuilder();
			int i = 0;
			foreach(var entity in entities)
			{
				builder.AppendLine("[" + i + "] " + entity.ToString());
				i++;
			}
			_Trace.TraceEvent(TraceEventType.Error, 0, "Error while importing entities (len:" + entities.Length + ") : " + Utils.ExceptionToString(ex) + "\r\n" + builder.ToString());
		}

		internal static void RetryWorked()
		{
			_Trace.TraceInformation("Retry worked");
		}

		internal static void LogProgress(ProgressTracker progress)
		{
			var info = "Progress : " + progress.CurrentProgress.ToString("0.000");
			_Trace.TraceInformation(info);
		}

		internal static void TaskCount(int count)
		{
			_Trace.TraceInformation("Upload thread count : " + count);
		}

		internal static void StartAtPosition(DiskBlockPos startPosition)
		{
			_Trace.TraceInformation("Start at position " + startPosition.ToString());
		}

		internal static void ProcessingSize(long size)
		{
			double inMb = (double)size / 1024.0 / 1024.0;
			_Trace.TraceInformation("MB to process : " + inMb.ToString("0.00"));
		}

		internal static void ErrorWhileImportingBalancesToAzure(Exception ex, string txid)
		{
			_Trace.TraceEvent(TraceEventType.Error, 0, "Error while importing balances on " + txid + " \r\n" + Utils.ExceptionToString(ex));
		}

		internal static void MissingTransactionFromDatabase(uint256 txid)
		{
			_Trace.TraceEvent(TraceEventType.Error, 0, "Missing transaction from index while fetching outputs " + txid);
		}


		internal static void LocalMainChainTip(uint256 blockId, int height)
		{
			_Trace.TraceInformation("Local main tip " + ToString(blockId, height));
		}

		private static string ToString(uint256 blockId, int height)
		{
			return "Height : " + height + ", BlockId : " + blockId;
		}

		internal static void RemoteMainChainTip(uint256 blockId, int height)
		{
			_Trace.TraceInformation("Remote main tip " + ToString(blockId, height));
		}

		internal static void LocalMainChainIsLate()
		{
			_Trace.TraceInformation("Local main chain is late");
		}

		public static void ImportingChain(ChainedBlock from, ChainedBlock to)
		{
			_Trace.TraceInformation("Importing blocks from " + ToString(from) + " to " + ToString(to) + " (both included)");
		}

		private static string ToString(ChainedBlock chainedBlock)
		{
			return ToString(chainedBlock.HashBlock, chainedBlock.Height);
		}

		internal static void RemainingBlockChain(int height, int maxHeight)
		{
			int remaining = height - maxHeight;
			if(remaining % 1000 == 0 && remaining != 0)
			{
				_Trace.TraceInformation("Remaining chain block to index : " + remaining + " (" + height + "/" + maxHeight + ")");
			}
		}
	}
}
