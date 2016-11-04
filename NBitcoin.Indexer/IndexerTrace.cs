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
            if (time.TotalSeconds == 0.0)
                time = TimeSpan.FromMilliseconds(10);
            double speed = ((double)bytes / 1024.0) / time.TotalSeconds;
            _Trace.TraceEvent(TraceEventType.Verbose, 0, "Block uploaded successfully (" + speed.ToString("0.00") + " KB/S)");
        }

        internal static TraceCorrelation NewCorrelation(string activityName)
        {
            var correlation = new TraceCorrelation(_Trace, activityName);
            _Trace.TraceInformation(activityName);
            return correlation;
        }

        internal static void CheckpointLoaded(ChainedBlock block, string checkpointName)
        {
            _Trace.TraceInformation("Checkpoint " + checkpointName + " loaded at " + ToString(block));
        }

        internal static void CheckpointSaved(ChainedBlock block, string checkpointName)
        {
            _Trace.TraceInformation("Checkpoint " + checkpointName + " saved at " + ToString(block));
        }


        internal static void ErrorWhileImportingEntitiesToAzure(ITableEntity[] entities, Exception ex)
        {
            StringBuilder builder = new StringBuilder();
            int i = 0;
            foreach (var entity in entities)
            {
                builder.AppendLine("[" + i + "] " + entity.RowKey);
                i++;
            }
            _Trace.TraceEvent(TraceEventType.Error, 0, "Error while importing entities (len:" + entities.Length + ") : " + Utils.ExceptionToString(ex) + "\r\n" + builder.ToString());
        }

        internal static void RetryWorked()
        {
            _Trace.TraceInformation("Retry worked");
        }

        public static string Pretty(TimeSpan span)
        {
            if (span == TimeSpan.Zero)
                return "0m";

            var sb = new StringBuilder();
            if (span.Days > 0)
                sb.AppendFormat("{0}d ", span.Days);
            if (span.Hours > 0)
                sb.AppendFormat("{0}h ", span.Hours);
            if (span.Minutes > 0)
                sb.AppendFormat("{0}m", span.Minutes);
            var result = sb.ToString();
            if (result == string.Empty)
                return "< 1min";
            return result;
        }

        internal static void TaskCount(int count)
        {
            _Trace.TraceInformation("Upload thread count : " + count);
        }

        internal static void ErrorWhileImportingBalancesToAzure(Exception ex, uint256 txid)
        {
            _Trace.TraceEvent(TraceEventType.Error, 0, "Error while importing balances on " + txid + " \r\n" + Utils.ExceptionToString(ex));
        }

        internal static void MissingTransactionFromDatabase(uint256 txid)
        {
            _Trace.TraceEvent(TraceEventType.Error, 0, "Missing transaction from index while fetching outputs " + txid);
        }


        internal static void InputChainTip(ChainedBlock block)
        {
            _Trace.TraceInformation("The input chain tip is at height " + ToString(block));
        }

        private static string ToString(uint256 blockId, int height)
        {
            return height.ToString();
        }

        internal static void IndexedChainTip(uint256 blockId, int height)
        {
            _Trace.TraceInformation("Indexed chain is at height " + ToString(blockId, height));
        }

        internal static void InputChainIsLate()
        {
            _Trace.TraceInformation("The input chain is late compared to the indexed one");
        }

        public static void IndexingChain(ChainedBlock from, ChainedBlock to)
        {
            _Trace.TraceInformation("Indexing blocks from " + ToString(from) + " to " + ToString(to) + " (both included)");
        }

        private static string ToString(ChainedBlock chainedBlock)
        {
            if (chainedBlock == null)
                return "(null)";
            return ToString(chainedBlock.HashBlock, chainedBlock.Height);
        }

        internal static void RemainingBlockChain(int height, int maxHeight)
        {
            int remaining = height - maxHeight;
            if (remaining % 1000 == 0 && remaining != 0)
            {
                _Trace.TraceInformation("Remaining chain block to index : " + remaining + " (" + height + "/" + maxHeight + ")");
            }
        }

        internal static void IndexedChainIsUpToDate(ChainedBlock block)
        {
            _Trace.TraceInformation("Indexed chain is up to date at height " + ToString(block));
        }

        public static void Information(string message)
        {
            _Trace.TraceInformation(message);
        }

        internal static void NoForkFoundWithStored()
        {
            _Trace.TraceInformation("No fork found with the stored chain");
        }

        public static void Processed(int height, int totalHeight, Queue<DateTime> lastLogs, Queue<int> lastHeights)
        {
            var lastLog = lastLogs.LastOrDefault();
            if (DateTime.UtcNow - lastLog > TimeSpan.FromSeconds(10))
            {
                if (lastHeights.Count > 0)
                {
                    var lastHeight = lastHeights.Last();
                    var time = DateTimeOffset.UtcNow - lastLogs.Last();
                    var remaining = totalHeight - height;
                    var block = height - lastHeight;
                    var blocksRemaining = remaining/block;
                    var remainingTime = blocksRemaining * (int)time.TotalSeconds;
                    var estimatedTime = totalHeight == 0 ? TimeSpan.FromDays(999.0) : TimeSpan.FromSeconds(remainingTime);

                    _Trace.TraceInformation("Blocks {0}/{1} (processed {2} blocks in: {3} seconds | estimate : {4})", height, totalHeight, block, (int)time.TotalSeconds, Pretty(estimatedTime));
                }
                lastLogs.Enqueue(DateTime.UtcNow);
                lastHeights.Enqueue(height);

                while (lastLogs.Count > 20)
                {
                    lastLogs.Dequeue();
                    lastHeights.Dequeue();
                }
            }
        }
    }
}
