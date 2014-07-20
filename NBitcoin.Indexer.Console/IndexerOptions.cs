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
		[Option('b', "ImportBlocksInAzure", DefaultValue = false, Required = false, HelpText = "Import blocks from data directory speicified in LocalSettings into azure container")]
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

		[Option('t', "ImportTransactionsInAzure", DefaultValue = false, Required = false, HelpText = "Import transactions from data directory speicified in LocalSettings into azure table")]
		public bool ImportTransactionsInAzure
		{
			get;
			set;
		}

		[Option('u', "UploadThreadCount", DefaultValue = -1, Required = false, HelpText = "Number of simultaneous uploads (default value is 15 for blocks upload, 30 for transactions upload)")]
		public int ThreadCount
		{
			get;
			set;
		}
	}
}
