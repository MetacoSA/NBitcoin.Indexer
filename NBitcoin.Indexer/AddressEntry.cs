using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace NBitcoin.Indexer
{
	public class AddressEntry
	{
		public class Entity : TableEntity
		{
			public static Dictionary<string, Entity> ExtractFromTransaction(Transaction tx, string txId)
			{
				return ExtractFromTransaction(null, tx, txId);
			}
			public static Dictionary<string, Entity> ExtractFromTransaction(string blockId, Transaction tx, string txId)
			{
				Dictionary<string, AddressEntry.Entity> entryByAddress = new Dictionary<string, AddressEntry.Entity>();
				foreach(var input in tx.Inputs)
				{
					if(tx.IsCoinBase)
						break;
					var signer = GetSigner(input.ScriptSig);
					if(signer != null)
					{
						AddressEntry.Entity entry = null;
						if(!entryByAddress.TryGetValue(signer.ToString(), out entry))
						{
							entry = new AddressEntry.Entity(txId, signer, blockId);
							entryByAddress.Add(signer.ToString(), entry);
						}
						entry.AddSend(input.PrevOut);
					}
				}

				int i = 0;
				foreach(var output in tx.Outputs)
				{
					var receiver = GetReciever(output.ScriptPubKey);
					if(receiver != null)
					{
						AddressEntry.Entity entry = null;
						if(!entryByAddress.TryGetValue(receiver.ToString(), out entry))
						{
							entry = new AddressEntry.Entity(txId, receiver, blockId);
							entryByAddress.Add(receiver.ToString(), entry);
						}
						entry.AddReceive(i);
					}
					i++;
				}
				foreach(var kv in entryByAddress)
					kv.Value.Flush();
				return entryByAddress;
			}

			private static BitcoinAddress GetReciever(Script scriptPubKey)
			{
				var payToHash = payToPubkeyHash.ExtractScriptPubKeyParameters(scriptPubKey);
				if(payToHash != null)
				{
					return new BitcoinAddress(payToHash, Network.Main);
				}

				var payToScript = payToScriptHash.ExtractScriptPubKeyParameters(scriptPubKey);
				if(payToScript != null)
				{
					return new BitcoinScriptAddress(payToScript, Network.Main);
				}
				return null;
			}





			static PayToPubkeyHashTemplate payToPubkeyHash = new PayToPubkeyHashTemplate();
			static PayToScriptHashTemplate payToScriptHash = new PayToScriptHashTemplate();
			private static BitcoinAddress GetSigner(Script scriptSig)
			{
				var pubKey = payToPubkeyHash.ExtractScriptSigParameters(scriptSig);
				if(pubKey != null)
				{
					return new BitcoinAddress(pubKey.PublicKey.ID, Network.Main);
				}
				var p2sh = payToScriptHash.ExtractScriptSigParameters(scriptSig);
				if(p2sh != null)
				{
					return new BitcoinScriptAddress(p2sh.RedeemScript.ID, Network.Main);
				}
				return null;
			}

			public Entity()
			{

			}
			public Entity(string txid, BitcoinAddress address, string blockId)
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
			void AddReceive(int n)
			{
				var nCompact = new CompactVarInt((ulong)n, 4);
				nCompact.ReadWrite(receiveStream, true);
			}

			MemoryStream outpointStream = new MemoryStream();
			void AddSend(OutPoint outpoint)
			{
				outpoint.ReadWrite(outpointStream, true);
			}
			void Flush()
			{
				SentOutpoints = Helper.GetBytes(outpointStream);
				ReceivedOutput = Helper.GetBytes(receiveStream);
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
				return Helper.DeserializeList<OutPoint>(SentOutpoints);
			}


			internal List<TxOut> GetReceivedTxOut()
			{
				return Helper.DeserializeList<TxOut>(ReceivedTxOuts);
			}


			public override string ToString()
			{
				return "RowKey : " + RowKey;
			}

			byte[] _SentTxOuts;
			[IgnoreProperty]
			public byte[] SentTxOuts
			{
				get
				{
					if(_SentTxOuts == null)
						_SentTxOuts = Helper.Concat(SentTxOuts1, SentTxOuts2, SentTxOuts3, SentTxOuts4);
					return _SentTxOuts;
				}
				set
				{
					_SentTxOuts = value;
					Helper.Spread(value, 1024 * 63, ref _SentTxOuts1, ref _SentTxOuts2, ref _SentTxOuts3, ref _SentTxOuts4);
				}
			}

			byte[] _SentTxOuts1;
			public byte[] SentTxOuts1
			{
				get
				{
					return _SentTxOuts1;
				}
				set
				{
					_SentTxOuts1 = value;
				}
			}
			byte[] _SentTxOuts2;
			public byte[] SentTxOuts2
			{
				get
				{
					return _SentTxOuts2;
				}
				set
				{
					_SentTxOuts2 = value;
				}
			}
			byte[] _SentTxOuts3;
			public byte[] SentTxOuts3
			{
				get
				{
					return _SentTxOuts3;
				}
				set
				{
					_SentTxOuts3 = value;
				}
			}
			byte[] _SentTxOuts4;
			public byte[] SentTxOuts4
			{
				get
				{
					return _SentTxOuts4;
				}
				set
				{
					_SentTxOuts4 = value;
				}
			}

			byte[] _ReceivedTxOuts;

			[IgnoreProperty]
			public byte[] ReceivedTxOuts
			{
				get
				{
					if(_ReceivedTxOuts == null)
						_ReceivedTxOuts = Helper.Concat(_ReceivedTxOuts1, _ReceivedTxOuts2, _ReceivedTxOuts3, _ReceivedTxOuts4);
					return _ReceivedTxOuts;
				}
				set
				{
					_ReceivedTxOuts = value;
					Helper.Spread(value, 1024 * 63, ref _ReceivedTxOuts1, ref _ReceivedTxOuts2, ref _ReceivedTxOuts3, ref _ReceivedTxOuts4);
				}
			}

			byte[] _ReceivedTxOuts1;
			public byte[] ReceivedTxOuts1
			{
				get
				{
					return _ReceivedTxOuts1;
				}
				set
				{
					_ReceivedTxOuts1 = value;
				}
			}
			byte[] _ReceivedTxOuts2;
			public byte[] ReceivedTxOuts2
			{
				get
				{
					return _ReceivedTxOuts2;
				}
				set
				{
					_ReceivedTxOuts2 = value;
				}
			}
			byte[] _ReceivedTxOuts3;
			public byte[] ReceivedTxOuts3
			{
				get
				{
					return _ReceivedTxOuts3;
				}
				set
				{
					_ReceivedTxOuts3 = value;
				}
			}

			byte[] _ReceivedTxOuts4;
			public byte[] ReceivedTxOuts4
			{
				get
				{
					return _ReceivedTxOuts4;
				}
				set
				{
					_ReceivedTxOuts4 = value;
				}
			}
			internal List<TxOut> GetSentTxOuts()
			{
				return Helper.DeserializeList<TxOut>(SentTxOuts);
			}
		}
		public uint256 TransactionId
		{
			get;
			set;
		}

		public BitcoinAddress Address
		{
			get;
			set;
		}

		public List<OutPoint> SpentOutpoints
		{
			get;
			set;
		}

		public uint256[] BlockIds
		{
			get;
			set;
		}

		public Money BalanceChange
		{
			get;
			set;
		}

		public List<TxOut> ReceivedTxOuts
		{
			get;
			set;
		}
		public List<TxOut> SpentTxOuts
		{
			get;
			set;
		}

		public override string ToString()
		{
			return Address + " - " + (BalanceChange == null ? "??" : BalanceChange.ToString());
		}

		public IEnumerable<OutPoint> ReceivedOutpoints
		{
			get
			{
				return ReceivedTxInIndex.Select(i => new OutPoint(TransactionId, i));
			}
		}
		public List<int> ReceivedTxInIndex
		{
			get;
			set;
		}

		public DateTimeOffset? MempoolDate
		{
			get;
			set;
		}
	}
}
