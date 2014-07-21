using Microsoft.WindowsAzure.Storage.Table;
using NBitcoin.DataEncoders;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NBitcoin.Indexer
{
	internal class IndexedAddressEntry : TableEntity
	{
		public IndexedAddressEntry(string txid, BitcoinAddress address)
		{
			var wif = address.ToString();
			PartitionKey = new string(wif.Skip(wif.Length - 3).ToArray());
			RowKey = wif + "-" + txid;
		}

		MemoryStream receiveStream = new MemoryStream();
		public void AddReceive(int n)
		{
			var nCompact = new CompactVarInt((ulong)n,4);
			nCompact.ReadWrite(receiveStream, true);
		}

		MemoryStream outpointStream = new MemoryStream();
		public void AddSend(OutPoint outpoint)
		{
			outpoint.ReadWrite(outpointStream, true);
		}
		public void Flush()
		{
			SentOutpoints = GetBytes(outpointStream);
			ReceivedOutput = GetBytes(receiveStream);
		}

		private byte[] GetBytes(MemoryStream stream)
		{
			if(stream.Length == 0)
				return null;
			var buffer = stream.GetBuffer();
			Array.Resize(ref buffer, (int)stream.Length);
			if(buffer.Length > 1024*64)
				throw new ArgumentOutOfRangeException("stream", "Value too big to enter in an Azure Table Column");
			return buffer;
		}

		public byte[] ReceivedOutput
		{
			get;
			set;
		}

		public byte[] SentOutpoints
		{
			get;
			set;
		}
	}
}
