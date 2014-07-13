using CommandLine;
using CommandLine.Text;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace NBitcoin.Indexer.Console
{
	class IndexerOptions
	{
		[Option("ImportBlocksInAzure", DefaultValue = false, Required = false, HelpText = "Import blocks from data directory speicified in LocalSettings into azure")]
		public bool ImportBlocksInAzure
		{
			get;
			set;
		}


		string _Usage;
		[HelpOption('?', "help", HelpText = "Display this help screen.")]
		public string GetUsage()
		{
			if(_Usage == null)
				_Usage = HelpText.AutoBuild(this, (HelpText current) => HelpText.DefaultParsingErrorsHandler(this, current));
			return _Usage;
			//
		}
	}
}
