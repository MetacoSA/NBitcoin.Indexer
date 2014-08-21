using NBitcoin.Protocol;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NBitcoin.Indexer.Tests
{
	public class MiniNode
	{

		public MiniNode(BlockStore blockStore, NodeServer server)
		{
			_Generator = new BlockGenerator(blockStore);
			_Server = server;
			server.AllMessages.AddMessageListener(new NewThreadMessageListener<IncomingMessage>(ProcessMessage));
		}

		private readonly NodeServer _Server;
		public NodeServer Server
		{
			get
			{
				return _Server;
			}
		}
		private readonly BlockGenerator _Generator;
		public BlockGenerator Generator
		{
			get
			{
				return _Generator;
			}
		}


		private void ProcessMessage(IncomingMessage message)
		{
			var getheader = message.Message.Payload as GetHeadersPayload;
			if(getheader != null)
			{
				ChainedBlock forkPos = null;
				int height = 0;
				foreach(var blk in getheader.BlockLocators.Blocks)
				{
					forkPos = Generator.Chain.GetBlock(blk);
					if(forkPos != null)
						break;
				}
				if(forkPos != null)
					height = forkPos.Height + 1;

				HeadersPayload getData = new HeadersPayload();
				while(height <= Generator.Chain.Height)
				{
					var block = Generator.Chain.GetBlock(height);
					getData.Headers.Add(block.Header);
					if(block.HashBlock == getheader.HashStop)
						break;
					height++;
				}
				message.Node.SendMessage(getData);
			}
		}
	}
}
