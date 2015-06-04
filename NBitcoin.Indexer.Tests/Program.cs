using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace NBitcoin.Indexer.Tests
{
    class Program
    {
        public static void Main(string[] args)
        {
            //var client = IndexerConfiguration.FromConfiguration().CreateIndexerClient();
            //var changes = client.GetOrderedBalance(BitcoinAddress.Create("1LuckyR1fFHEsXYyx5QK4UFzv3PEAepPMK"));
            //foreach (var change in changes)
            //{
            //}
            //var result = client.GetAddressBalance(new BitcoinAddress("1dice8EMZmqKvrGE4Qc9bUFf9PX3xaYDp"));
            
            //Azure();
            //SqlLite();
            new TestClass().Play();
        }

     

        internal static void SetThrottling()
        {
            ServicePointManager.UseNagleAlgorithm = false;
            ServicePointManager.Expect100Continue = false;
            ServicePointManager.DefaultConnectionLimit = 100;
        }
        private static void Azure()
        {
            SetThrottling();
            var client = IndexerConfiguration.FromConfiguration().CreateBlobClient();
            var container = client.GetContainerReference("throughput2");
            container.CreateIfNotExists();
            int count = 0;

            var threads = Enumerable.Range(0, 30).Select(_ => new Thread(__ =>
            {

                while (true)
                {
                    var testStream = new MemoryStream(new byte[] { 1, 2, 3, 4, 5, 6 });
                    // Create a random blob name.
                    string blobName = string.Format("test-{0}.txt", Guid.NewGuid());

                    // Get a reference to the blob storage system.
                    var blobReference = container.GetBlockBlobReference(blobName);

                    // Upload the word "hello" from a Memory Stream.
                    blobReference.UploadFromStream(testStream);

                    // Increment my stat-counter.
                    Interlocked.Increment(ref count);
                }
            })).ToArray();
            foreach (var test in threads)
            {
                test.Start();
            }

            while (true)
            {
                Thread.Sleep(2000);
                Console.WriteLine(count + " / s");
                Interlocked.Exchange(ref count, 0);
            }
        }
    }
}
