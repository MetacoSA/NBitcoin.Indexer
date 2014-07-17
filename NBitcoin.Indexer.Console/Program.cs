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
				if(options.ImportBlocksInAzure)
				{
					var importer = AzureBlockImporter.CreateBlockImporter();
					importer.StartBlockImportToAzure();
				}
			}
		}
	}
}
