using CommandLine;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NBitcoin.Indexer.Console
{
    class Program
    {
        static void Main(string[] args)
        {
            try
            {

                var options = new IndexerOptions();
                if (args.Length == 0)
                    System.Console.WriteLine(options.GetUsage());
                if (Parser.Default.ParseArguments(args, options))
                {
                    System.Console.WriteLine("NBitcoin.Indexer " + typeof(AzureIndexer).Assembly.GetName().Version);
                    var indexer = AzureIndexer.CreateIndexer();
                    indexer.CheckpointInterval = TimeSpan.Parse(options.CheckpointInterval);
                    indexer.NoSave = options.NoSave;
                    indexer.FromHeight = options.From;
                    indexer.ToHeight = options.To;
                    indexer.TaskCount = options.ThreadCount;

                    ChainBase chain = null;
                    if (options.IndexBlocks)
                    {
                        indexer.IndexBlocks();
                    }
                    if (options.IndexChain)
                    {
                        chain = indexer.GetNodeChain();
                        try
                        {
                            indexer.IndexChain(chain);
                        }
                        finally
                        {
                            ((Chain)chain).Changes.Dispose();
                        }
                    }
                    if (options.IndexTransactions)
                    {
                        indexer.IndexTransactions();
                    }
                    if (options.IndexAddresses)
                    {
                        chain = chain ?? indexer.Configuration.CreateIndexerClient().GetMainChain();
                        indexer.IndexOrderedBalances(chain);
                    }
                    if (options.IndexWallets)
                    {
                        chain = chain ?? indexer.Configuration.CreateIndexerClient().GetMainChain();
                        indexer.IndexWalletBalances(chain);
                    }
                }
            }
            catch (ConfigurationErrorsException ex)
            {
                System.Console.WriteLine("LocalSettings.config missing settings : " + ex.Message);
            }
        }
    }
}
