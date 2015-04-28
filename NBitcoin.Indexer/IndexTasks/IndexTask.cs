using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Runtime.ExceptionServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace NBitcoin.Indexer.IndexTasks
{
    public interface IIndexTask
    {
        Task IndexAsync(BlockFetcher blockFetcher);
        bool SaveProgression
        {
            get;
            set;
        }
        bool EnsureIsSetup
        {
            get;
            set;
        }
    }
    public abstract class IndexTask<TIndexed> : IIndexTask
    {
        int _RunningTask = 0;

        volatile Exception _IndexingException;

        

        public void Index(BlockFetcher blockFetcher)
        {
            try
            {
                IndexAsync(blockFetcher).Wait();
            }
            catch (AggregateException aex)
            {
                ExceptionDispatchInfo.Capture(aex.InnerException).Throw();
                throw;
            }
        }


        bool _EnsureIsSetup = true;
        public bool EnsureIsSetup
        {
            get
            {
                return _EnsureIsSetup;
            }
            set
            {
                _EnsureIsSetup = value;
            }
        }
        public async Task IndexAsync(BlockFetcher blockFetcher)
        {
            SetThrottling();
            if (EnsureIsSetup)
                await EnsureSetup().ConfigureAwait(false);
            BulkImport<TIndexed> bulk = new BulkImport<TIndexed>(PartitionSize);
            foreach (var block in blockFetcher)
            {
                ThrowIfException();
                if (bulk.IsComplete)
                    continue;

                if (blockFetcher.NeedSave)
                {
                    if (!SaveProgression)
                    {
                        EnqueueTasks(bulk, true);
                        await SaveAsync(blockFetcher, bulk).ConfigureAwait(false);
                    }
                }
                ProcessBlock(block, bulk);
                if (bulk.HasFullPartition)
                {
                    EnqueueTasks(bulk, false);
                }
            }
            EnqueueTasks(bulk, true);
            if (!SaveProgression)
                await SaveAsync(blockFetcher, bulk).ConfigureAwait(false);
            await WaitRunningTaskIsBelow(0).ConfigureAwait(false);
        }

        private void SetThrottling()
        {
            Helper.SetThrottling();
            ServicePoint tableServicePoint = ServicePointManager.FindServicePoint(Configuration.CreateTableClient().BaseUri);
            tableServicePoint.ConnectionLimit = 1000;
        }
        ExponentialBackoff retry = new ExponentialBackoff(15, TimeSpan.FromMilliseconds(100),
                                                              TimeSpan.FromSeconds(10),
                                                              TimeSpan.FromMilliseconds(200));
        private void EnqueueTasks(BulkImport<TIndexed> bulk, bool uncompletePartitions)
        {
            if (!uncompletePartitions && !bulk.HasFullPartition)
                return;
            if (uncompletePartitions)
                bulk.FlushUncompletePartitions();

            int runningTask = Interlocked.CompareExchange(ref _RunningTask, 0, 0);
            if (runningTask > 500)
                WaitRunningTaskIsBelow(70).Wait();

            while (bulk._ReadyPartitions.Count != 0)
            {
                var item = bulk._ReadyPartitions.Dequeue();
                var task = retry.Do(() => IndexCore(item.Item1, item.Item2));
                Interlocked.Increment(ref _RunningTask);
                task.ContinueWith(t =>
                {
                    if (t.Exception != null)
                    {
                        _IndexingException = t.Exception.InnerException;
                    }
                    Interlocked.Decrement(ref _RunningTask);
                });
            }
        }

        private async Task SaveAsync(BlockFetcher fetcher, BulkImport<TIndexed> bulk)
        {
            await WaitRunningTaskIsBelow(0);
            ThrowIfException();
            fetcher.SaveCheckpoint();
        }

        int[] wait = new int[] { 100, 200, 400, 800, 1600 };
        private async Task WaitRunningTaskIsBelow(int taskCount)
        {
            int i = 0;
            while (true)
            {

                int runningTask = Interlocked.CompareExchange(ref _RunningTask, 0, 0);
                if (runningTask <= taskCount)
                    break;
                await Task.Delay(wait[Math.Min(wait.Length - 1, i)]).ConfigureAwait(false);
                i++;
            }
        }

        private void ThrowIfException()
        {
            if (_IndexingException != null)
                ExceptionDispatchInfo.Capture(_IndexingException).Throw();
        }


        protected TimeSpan _Timeout = TimeSpan.FromMinutes(5.0);
        public IndexerConfiguration Configuration
        {
            get;
            private set;
        }
        public bool SaveProgression
        {
            get;
            set;
        }

        protected abstract int PartitionSize
        {
            get;
        }


        protected abstract Task EnsureSetup();
        protected abstract void ProcessBlock(BlockInfo block, BulkImport<TIndexed> bulk);
        protected abstract Task IndexCore(string partitionName, IEnumerable<TIndexed> items);

        public IndexTask(IndexerConfiguration configuration)
        {
            if (configuration == null)
                throw new ArgumentNullException("configuration");
            this.Configuration = configuration;
        }
    }
}
