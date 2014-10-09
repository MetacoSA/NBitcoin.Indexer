using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace NBitcoin.Indexer
{
    public class TaskPool<TItem>
    {
        public TaskPool(BlockingCollection<TItem> collection, Action<TItem> action, int defaultTaskCount)
        {
            this.defaultTaskCount = defaultTaskCount;
            this.collection = collection;
            this.action = action;
        }

        public int TaskCount
        {
            get;
            set;
        }

        public void Start()
        {
            if (_Tasks != null)
                throw new InvalidOperationException("Already started");
            _Source = new CancellationTokenSource();
            _Tasks =
                Enumerable.Range(0, TaskCount == -1 ? defaultTaskCount : TaskCount).Select(_ => Task.Factory.StartNew(() =>
                {
                    try
                    {
                        foreach (var item in collection.GetConsumingEnumerable(_Source.Token))
                        {
                            action(item);
                        }
                    }
                    catch (OperationCanceledException)
                    {
                    }
                }, TaskCreationOptions.LongRunning)).ToArray();
        }

        CancellationTokenSource _Source = new CancellationTokenSource();

        public void Stop()
        {
            if (_Tasks != null)
            {
                WaitProcessed(collection);
                _Source.Cancel();
                Task.WaitAll(_Tasks);
                _Tasks = null;
            }
        }

        private void WaitProcessed<T>(BlockingCollection<T> collection)
        {
            while (collection.Count != 0)
            {
                Thread.Sleep(1000);
            }
        }

        Task[] _Tasks;
        public Task[] Tasks
        {
            get
            {
                return _Tasks;
            }
        }
        private int defaultTaskCount;
        private BlockingCollection<TItem> collection;
        private Action<TItem> action;
    }
}
