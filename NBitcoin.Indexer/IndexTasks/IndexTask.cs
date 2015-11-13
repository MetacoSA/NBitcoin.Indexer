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
        void Index(BlockFetcher blockFetcher);
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
        volatile Exception _IndexingException;

        /// <summary>
        /// Fast forward indexing to the end (if scanning not useful)
        /// </summary>
        protected virtual bool SkipToEnd
        {
            get
            {
                return false;
            }
        }



        public void Index(BlockFetcher blockFetcher)
        {
            try
            {
                SetThrottling();
                if (EnsureIsSetup)
                    EnsureSetup().Wait();


                using (CustomThreadPoolTaskScheduler scheduler = CreateScheduler())
                {
                    BulkImport<TIndexed> bulk = new BulkImport<TIndexed>(PartitionSize);
                    if (!SkipToEnd)
                    {
                        try
                        {

                            foreach(var block in blockFetcher)
                            {
                                ThrowIfException();
                                if(blockFetcher.NeedSave)
                                {
                                    if(SaveProgression)
                                    {
                                        EnqueueTasks(bulk, true, scheduler);
                                        Save(blockFetcher, bulk, scheduler);
                                    }
                                }
                                ProcessBlock(block, bulk);
                                if(bulk.HasFullPartition)
                                {
                                    EnqueueTasks(bulk, false, scheduler);
                                }
                            }
                            EnqueueTasks(bulk, true, scheduler);
                        }
                        catch(OperationCanceledException ex)
                        {
                            if(ex.CancellationToken != blockFetcher.CancellationToken)
                                throw;
                        }
                    }
                    else
                        blockFetcher.SkipToEnd();
                    if (SaveProgression)
                        Save(blockFetcher, bulk, scheduler);
                    WaitFinished(scheduler);
                }
            }
            catch (AggregateException aex)
            {
                ExceptionDispatchInfo.Capture(aex.InnerException).Throw();
                throw;
            }
        }

        protected CustomThreadPoolTaskScheduler CreateScheduler()
        {
            CustomThreadPoolTaskScheduler scheduler = new CustomThreadPoolTaskScheduler(ThreadCount, MaxQueued);
            return scheduler;
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

        private void SetThrottling()
        {
            Helper.SetThrottling();
            ServicePoint tableServicePoint = ServicePointManager.FindServicePoint(Configuration.CreateTableClient().BaseUri);
            tableServicePoint.ConnectionLimit = 1000;
        }
        ExponentialBackoff retry = new ExponentialBackoff(15, TimeSpan.FromMilliseconds(100),
                                                              TimeSpan.FromSeconds(10),
                                                              TimeSpan.FromMilliseconds(200));
        private void EnqueueTasks(BulkImport<TIndexed> bulk, bool uncompletePartitions, CustomThreadPoolTaskScheduler scheduler)
        {
            if (!uncompletePartitions && !bulk.HasFullPartition)
                return;
            if (uncompletePartitions)
                bulk.FlushUncompletePartitions();

            while (bulk._ReadyPartitions.Count != 0)
            {
                var item = bulk._ReadyPartitions.Dequeue();
                var task = retry.Do(() => IndexCore(item.Item1, item.Item2), scheduler);
                task.ContinueWith(t =>
                {
                    if (t.Exception != null)
                    {
                        _IndexingException = t.Exception.InnerException;
                    }
                });
            }
        }

        private void Save(BlockFetcher fetcher, BulkImport<TIndexed> bulk, CustomThreadPoolTaskScheduler scheduler)
        {
            WaitFinished(scheduler);
            fetcher.SaveCheckpoint();
        }

        int[] wait = new int[] { 100, 200, 400, 800, 1600 };
        private void WaitFinished(CustomThreadPoolTaskScheduler taskScheduler)
        {
            taskScheduler.WaitFinished();
            ThrowIfException();
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
        protected abstract void IndexCore(string partitionName, IEnumerable<TIndexed> items);

        public IndexTask(IndexerConfiguration configuration)
        {
            if (configuration == null)
                throw new ArgumentNullException("configuration");
            ThreadCount = 50;
            MaxQueued = 100;
            this.Configuration = configuration;
            SaveProgression = true;
        }

        public int ThreadCount
        {
            get;
            set;
        }

        public int MaxQueued
        {
            get;
            set;
        }
    }
}
