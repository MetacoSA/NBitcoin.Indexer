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
		[Option('b', "IndexBlocks", DefaultValue = false, Required = false, HelpText = "Index blocks into azure blob container")]
		public bool IndexBlocks
		{
			get;
			set;
		}

		[Option("NoSave", HelpText = "Do not save progress in a checkpoint file", Required = false, DefaultValue = false)]
		public bool NoSave
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

		[Option('c', "CountBlkFiles", HelpText = "Count the number of blk file downloaded by bitcoinq", DefaultValue = false, Required = false)]
		public bool CountBlkFiles
		{
			get;
			set;
		}

		[Option("FromBlk",
			HelpText = "The blk file where processing will start",
			DefaultValue = 0,
			Required = false)]
		public int FromBlk
		{
			get;
			set;
		}

		[Option("CountBlk",
			Required = false,
			DefaultValue = 999999,
			HelpText = "The number of blk file that must be processed")]
		public int BlkCount
		{
			get;
			set;
		}

		[Option('t', "IndexTransactions", DefaultValue = false, Required = false, HelpText = "Index transactions into azure table")]
		public bool IndexTransactions
		{
			get;
			set;
		}

		[Option('a', "IndexAddresses", DefaultValue = false, Required = false, HelpText = "Index bitcoin addresses into azure table")]
		public bool IndexAddresses
		{
			get;
			set;
		}

		[Option('m', "IndexMainChain", DefaultValue = false, Required = false, HelpText = "Index the main chain into azure table")]
		public bool IndexChain
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
