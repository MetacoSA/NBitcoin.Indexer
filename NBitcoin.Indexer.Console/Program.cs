using CommandLine;
using System;
using System.Collections.Generic;
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
			var options = new IndexerOptions();
			if(args.Length == 0)
				System.Console.WriteLine(options.GetUsage());
			if(Parser.Default.ParseArguments(args, options))
			{
				var importer = AzureIndexer.CreateBlockImporter();
				importer.NoSave = options.NoSave;
				importer.FromBlk = options.FromBlk;
				importer.BlkCount = options.BlkCount;
				importer.TaskCount = options.ThreadCount;
				if(options.ImportBlocksInAzure)
				{
					importer.StartBlockImportToAzure();
				}
				if(options.ImportTransactionsInAzure)
				{
					importer.StartTransactionImportToAzure();
				}
				if(options.CountBlkFiles)
				{
					var dir = new DirectoryInfo(importer.Configuration.BlockDirectory);
					if(!dir.Exists)
					{
						System.Console.WriteLine(dir.FullName + " does not exists");
						return;
					}
					System.Console.WriteLine("Blk files count : " +
						dir
						.GetFiles()
						.Where(f => f.Name.EndsWith(".dat"))
						.Where(f => f.Name.StartsWith("blk")).Count());
				}
			}
		}
	}
}
