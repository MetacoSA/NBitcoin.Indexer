using CommandLine;
using System;
using System.Collections.Generic;
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
			if(Parser.Default.ParseArguments(args, options))
			{
			}
		}
	}
}
