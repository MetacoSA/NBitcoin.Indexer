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
		public IndexedAddressEntry()
		{

		}
		public IndexedAddressEntry(string txid, BitcoinAddress address, string blockId)
		{
			var wif = address.ToString();
			PartitionKey = GetPartitionKey(wif);
			RowKey = wif + "-" + txid + "-" + blockId;
		}

		public static string GetPartitionKey(string wif)
		{
			char[] c = new char[3];
			c[0] = (int)(wif[wif.Length - 3]) % 2 == 0 ? 'a' : 'b';
			c[1] = wif[wif.Length - 2];
			c[2] = wif[wif.Length - 1];
			return new string(c);
		}

		MemoryStream receiveStream = new MemoryStream();
		public void AddReceive(int n)
		{
			var nCompact = new CompactVarInt((ulong)n, 4);
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
			if(buffer.Length > 1024 * 64)
				throw new ArgumentOutOfRangeException("stream", "Value too big to enter in an Azure Table Column");
			return buffer;
		}

		public string TransactionId
		{
			get
			{
				return RowKey.Split('-')[1];
			}
		}
		public string BlockId
		{
			get
			{
				var splitted = RowKey.Split('-');
				if(splitted.Length < 3)
					return "";
				return RowKey.Split('-')[2];
			}
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


		public List<int> GetReceivedOutput()
		{
			List<int> indexes = new List<int>();
			if(ReceivedOutput == null)
				return indexes;
			MemoryStream ms = new MemoryStream(ReceivedOutput);
			ms.Position = 0;
			while(ms.Position != ms.Length)
			{
				CompactVarInt value = new CompactVarInt(4);
				value.ReadWrite(ms, false);
				indexes.Add((int)value.ToLong());
			}
			return indexes;
		}

		public List<OutPoint> GetSentOutpoints()
		{
			List<OutPoint> outpoints = new List<OutPoint>();
			if(SentOutpoints == null)
				return outpoints;
			MemoryStream ms = new MemoryStream(SentOutpoints);
			ms.Position = 0;
			while(ms.Position != ms.Length)
			{
				OutPoint outpoint = new OutPoint();
				outpoint.ReadWrite(ms, false);
				outpoints.Add(outpoint);
			}
			return outpoints;
		}


		public string Money
		{
			get;
			set;
		}

		public override string ToString()
		{
			return "RowKey : " + RowKey;
		}
	}
}
