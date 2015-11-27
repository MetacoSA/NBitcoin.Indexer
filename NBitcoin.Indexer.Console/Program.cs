using CommandLine;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
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

                    if (options.All)
                    {
                        options.IndexAddresses = true;
                        options.IndexBlocks = true;
                        options.IndexWallets = true;
                        options.IndexChain = true;
                        options.IndexTransactions = true;
                    }

                    var indexer = AzureIndexer.CreateIndexer();
                    indexer.Configuration.EnsureSetup();
                    indexer.TaskScheduler = new CustomThreadPoolTaskScheduler(30, 100);
                    indexer.CheckpointInterval = TimeSpan.Parse(options.CheckpointInterval);
                    indexer.IgnoreCheckpoints = options.IgnoreCheckpoints;
                    indexer.FromHeight = options.From;
                    indexer.ToHeight = options.To;

                    ChainBase chain = null;
                    var checkpointRepository = indexer.GetCheckpointRepository();
                    checkpointRepository.CheckpointSet = null;
                    if (options.ListCheckpoints)
                    {
                        foreach (var checkpoint in checkpointRepository.GetCheckpointsAsync().Result)
                        {
                            chain = chain ?? indexer.GetNodeChain();
                            var fork = chain.FindFork(checkpoint.BlockLocator);
                            System.Console.WriteLine("Name : " + checkpoint.CheckpointName);
                            if (fork != null)
                            {
                                System.Console.WriteLine("Height : " + fork.Height);
                                System.Console.WriteLine("Hash : " + fork.HashBlock);
                            }
                            System.Console.WriteLine();

                        }
                    }
                    if (options.DeleteCheckpoint != null)
                    {
                        checkpointRepository.GetCheckpoint(options.DeleteCheckpoint).DeleteAsync().Wait();
                        System.Console.WriteLine("Checkpoint " + options.DeleteCheckpoint + " deleted");
                    }
                    if (options.AddCheckpoint != null)
                    {
                        chain = chain ?? indexer.GetNodeChain();
                        var split = options.AddCheckpoint.Split(':');
                        var name = split[0];
                        var height = int.Parse(split[1]);
                        var b = chain.GetBlock(height);

                        var checkpoint = checkpointRepository.GetCheckpoint(name);
                        checkpoint.SaveProgress(b.GetLocator());
                        System.Console.WriteLine("Checkpoint " + options.AddCheckpoint + " saved to height " + b.Height);
                    }
                    if (ConfigurationManager.AppSettings["MainDirectory"] != null)
                    {
                        System.Console.WriteLine("Warning : obsolete appsetting detected, MainDirectory");
                        string[] oldCheckpoints = new string[] { "transactions", "blocks", "wallets", "balances" };
                        foreach (var chk in oldCheckpoints)
                        {
                            var path = GetFilePath(indexer.Configuration, chk);
                            if (File.Exists(path))
                            {
                                var onlineCheckpoint = checkpointRepository.GetCheckpointsAsync().Result.FirstOrDefault(r => r.CheckpointName.ToLowerInvariant() == chk);
                                if (onlineCheckpoint == null)
                                {
                                    onlineCheckpoint = checkpointRepository.GetCheckpoint(indexer.Configuration.CheckpointSetName + "/" + chk);
                                    BlockLocator offlineLocator = new BlockLocator();
                                    offlineLocator.FromBytes(File.ReadAllBytes(path));
                                    onlineCheckpoint.SaveProgress(offlineLocator);
                                    System.Console.WriteLine("Local checkpoint " + chk + " saved in azure");
                                }
                                File.Delete(path);
                                System.Console.WriteLine("Checkpoint File deleted " + path);
                            }
                        }
                    }


                    if (options.IndexBlocks)
                    {
                        chain = chain ?? indexer.GetNodeChain();
                        indexer.IndexBlocks(chain);
                    }
                    if (options.IndexTransactions)
                    {
                        chain = chain ?? indexer.GetNodeChain();
                        indexer.IndexTransactions(chain);
                    }
                    if (options.IndexAddresses)
                    {
                        chain = chain ?? indexer.GetNodeChain();
                        indexer.IndexOrderedBalances(chain);
                    }
                    if (options.IndexWallets)
                    {
                        chain = chain ?? indexer.GetNodeChain();
                        indexer.IndexWalletBalances(chain);
                    }
                    if (options.IndexChain)
                    {
                        chain = chain ?? indexer.GetNodeChain();
                        indexer.IndexChain(chain);
                    }
                }
            }
            catch (ConfigurationErrorsException ex)
            {
                System.Console.WriteLine("LocalSettings.config missing settings : " + ex.Message);
            }
        }


        static string GetFilePath(IndexerConfiguration conf, string name)
        {
            var mainDirectory = ConfigurationManager.AppSettings["MainDirectory"];
            var fileName = conf.StorageNamespace + "-" + name;
            if (!String.IsNullOrEmpty(mainDirectory))
                return Path.Combine(mainDirectory, fileName);
            return fileName;
        }
    }
}
