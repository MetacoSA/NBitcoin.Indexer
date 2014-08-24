using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NBitcoin.Indexer
{
	public class IndexerClient
	{
		private readonly IndexerConfiguration _Configuration;
		public IndexerConfiguration Configuration
		{
			get
			{
				return _Configuration;
			}
		}

		public IndexerClient(IndexerConfiguration configuration)
		{
			if(configuration == null)
				throw new ArgumentNullException("configuration");
			_Configuration = configuration;
		}


		public Block GetBlock(uint256 blockId)
		{
			var ms = new MemoryStream();
			var container = Configuration.GetBlocksContainer();
			try
			{

				container.GetPageBlobReference(blockId.ToString()).DownloadToStream(ms);
				ms.Position = 0;
				Block b = new Block();
				b.ReadWrite(ms, false);
				return b;
			}
			catch(StorageException ex)
			{
				if(ex.RequestInformation != null && ex.RequestInformation.HttpStatusCode == 404)
				{
					return null;
				}
				throw;
			}
		}

		public IndexedTransactionEntry GetTransaction(bool lazyLoadSpentOutput, uint256 txId)
		{
			return GetTransactions(lazyLoadSpentOutput, new uint256[] { txId }).First();
		}
		public IndexedTransactionEntry GetTransaction(uint256 txId)
		{
			return GetTransactions(true, new[] { txId }).First();
		}

		/// <summary>
		/// Get transactions in Azure Table
		/// </summary>
		/// <param name="txIds"></param>
		/// <returns>All transactions (with null entries for unfound transactions)</returns>
		public IndexedTransactionEntry[] GetTransactions(bool lazyLoadSpentOutput, uint256[] txIds)
		{
			var result = new IndexedTransactionEntry[txIds.Length];
			var queries = new TableQuery<IndexedTransactionEntry.Entity>[txIds.Length];
			try
			{
				Parallel.For(0, txIds.Length, i =>
				{
					var table = Configuration.GetTransactionTable();
					queries[i] = new TableQuery<IndexedTransactionEntry.Entity>()
									.Where(
											TableQuery.CombineFilters(
												TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, IndexedTransactionEntry.Entity.GetPartitionKey(txIds[i])),
												TableOperators.And,
												TableQuery.CombineFilters(
													TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.GreaterThan, txIds[i].ToString() + "-"),
													TableOperators.And,
													TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.LessThan, txIds[i].ToString() + "|")
												)
										  ));

					var entities = table.ExecuteQuery(queries[i]).ToArray();
					if(entities.Length == 0)
						result[i] = null;
					else
					{
						result[i] = new IndexedTransactionEntry(entities);
						if(result[i].Transaction == null)
						{
							foreach(var block in result[i].BlockIds.Select(id => GetBlock(id)).Where(b => b != null))
							{
								result[i].Transaction = block.Transactions.FirstOrDefault(t => t.GetHash() == txIds[i]);
								entities[0].TransactionBytes = result[i].Transaction.ToBytes();
								if(entities[0].TransactionBytes.Length < 1024 * 64 * 4)
									table.Execute(TableOperation.Merge(entities[0]));
								break;
							}
						}

						var needTxOut = result[i].SpentTxOuts == null && lazyLoadSpentOutput && result[i].Transaction != null;
						if(needTxOut)
						{
							var tasks =
								result[i].Transaction
									 .Inputs
									 .Select(txin => Task.Run(() =>
									 {
										 var fromTx = GetTransactions(false, new uint256[] { txin.PrevOut.Hash }).FirstOrDefault();
										 if(fromTx == null)
										 {
											 IndexerTrace.MissingTransactionFromDatabase(txin.PrevOut.Hash);
											 return null;
										 }
										 return fromTx.Transaction.Outputs[(int)txin.PrevOut.N];
									 }))
									 .ToArray();

							Task.WaitAll(tasks);
							if(tasks.All(t => t.Result != null))
							{
								var outputs = tasks.Select(t => t.Result).ToArray();
								result[i].SpentTxOuts = outputs;
								entities[0].SpentOutputs = Helper.SerializeList(outputs);
								if(entities[0].SpentOutputs != null)
									table.Execute(TableOperation.Merge(entities[0]));
							}
							else
							{
								if(result[i].Transaction.IsCoinBase)
								{
									result[i].SpentTxOuts = new TxOut[0];
									entities[0].SpentOutputs = Helper.SerializeList(new TxOut[0]);
									if(entities[0].SpentOutputs != null)
										table.Execute(TableOperation.Merge(entities[0]));
								}
							}

						}

						if(result[i].Transaction == null)
							result[i] = null;
					}
				});
			}
			catch(AggregateException ex)
			{
				throw ex.InnerException;
			}
			return result;
		}

		public ChainChangeEntry GetBestBlock()
		{
			var table = Configuration.GetChainTable();
			var query = new TableQuery<ChainChangeEntry.Entity>()
						.Take(1);
			var entity = table.ExecuteQuery<ChainChangeEntry.Entity>(query).FirstOrDefault();
			if(entity == null)
				return null;
			return entity.ToObject();
		}

		public IEnumerable<ChainChangeEntry> GetChainChangesUntilFork(ChainedBlock currentTip, bool forkIncluded)
		{
			var table = Configuration.GetChainTable();
			var query = new TableQuery<ChainChangeEntry.Entity>();
			List<ChainChangeEntry> blocks = new List<ChainChangeEntry>();
			foreach(var block in table.ExecuteQuery(query).Select(e => e.ToObject()))
			{
				if(block.Height > currentTip.Height)
					yield return block;
				else if(block.Height < currentTip.Height)
				{
					currentTip = currentTip.FindAncestorOrSelf(block.Height);
				}

				if(block.Height == currentTip.Height)
				{
					if(block.BlockId == currentTip.HashBlock)
					{
						if(forkIncluded)
							yield return block;
						break;
					}
					else
					{
						yield return block;
						currentTip = currentTip.Previous;
					}
				}
			}
		}


		public AddressEntry[] GetEntries(BitcoinAddress address)
		{
			var addressStr = address.ToString();
			var table = Configuration.GetBalanceTable();
			var query = new TableQuery<AddressEntry.Entity>()
							.Where(
							TableQuery.CombineFilters(
												TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, AddressEntry.Entity.GetPartitionKey(addressStr)),
												TableOperators.And,
												TableQuery.CombineFilters(
													TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.GreaterThan, addressStr + "-"),
													TableOperators.And,
													TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.LessThan, addressStr + "|")
												)
							));

			var indexedEntriesGroups = table
									.ExecuteQuery(query)
									.GroupBy(e => e.TransactionId);
			List<AddressEntry> result = new List<AddressEntry>();
			foreach(var indexEntryGroup in indexedEntriesGroups)
			{
				var indexEntry = indexEntryGroup.First();
				var entry = new AddressEntry();
				entry.Address = address;
				entry.TransactionId = new uint256(indexEntry.TransactionId);
				if(indexEntry.ReceivedTxOuts == null)
					if(LazyLoad(indexEntry))
					{
						table.Execute(TableOperation.Merge(indexEntry));
					}
				entry.BlockIds = indexEntryGroup
										.Where(s => !string.IsNullOrEmpty(s.BlockId))
										.Select(s => new uint256(s.BlockId)).ToArray();
				entry.ReceivedTxOuts = indexEntry.GetReceivedTxOut();
				entry.ReceivedTxInIndex = indexEntry.GetReceivedOutput();

				entry.SpentOutpoints = indexEntry.GetSentOutpoints();
				entry.SpentTxOuts = indexEntry.GetSentTxOuts();
				entry.MempoolDate = indexEntryGroup.Where(e => string.IsNullOrEmpty(e.BlockId)).Select(e => e.Timestamp).FirstOrDefault();
				entry.BalanceChange = (indexEntry.SentTxOuts == null || indexEntry.ReceivedTxOuts == null) ? null : entry.ReceivedTxOuts.Select(t => t.Value).Sum() - entry.SpentTxOuts.Select(t => t.Value).Sum();
				result.Add(entry);
			}
			return result.ToArray();
		}

		private bool LazyLoad(AddressEntry.Entity indexAddress)
		{
			var txId = new uint256(indexAddress.TransactionId);
			var indexedTx = GetTransaction(txId);
			if(indexedTx == null)
				return false;

			Money total = Money.Zero;

			var received = indexAddress.GetReceivedOutput();
			indexAddress.ReceivedTxOuts =
							Helper.SerializeList(indexedTx.Transaction.Outputs.Where((o, i) => received.Contains(i))
							.ToList());


			Dictionary<uint256, Transaction> transactionsCache = new Dictionary<uint256, Transaction>();
			transactionsCache.Add(indexedTx.Transaction.GetHash(), indexedTx.Transaction);

			List<TxOut> sentTxOut = new List<TxOut>();
			var sentOutputs = indexAddress.GetSentOutpoints();

			foreach(var sent in sentOutputs)
			{
				Transaction sourceTransaction = null;
				if(!transactionsCache.TryGetValue(sent.Hash, out sourceTransaction))
				{
					var sourceIndexedTx = GetTransactions(false, new uint256[] { sent.Hash }).FirstOrDefault();
					if(sourceIndexedTx != null)
					{
						sourceTransaction = sourceIndexedTx.Transaction;
						transactionsCache.Add(sent.Hash, sourceTransaction);
					}
				}
				if(sourceTransaction == null || sourceTransaction.Outputs.Count <= sent.N)
				{
					return false;
				}
				sentTxOut.Add(sourceTransaction.Outputs[(int)sent.N]);
			}

			indexAddress.SentTxOuts = Helper.SerializeList(sentTxOut);
			return true;
		}
		public AddressEntry[] GetEntries(KeyId keyId)
		{
			return GetEntries(new BitcoinAddress(keyId, Configuration.Network));
		}
		public AddressEntry[] GetEntries(ScriptId scriptId)
		{
			return GetEntries(new BitcoinScriptAddress(scriptId, Configuration.Network));
		}
		public AddressEntry[] GetEntries(BitcoinScriptAddress scriptAddress)
		{
			return GetEntries((BitcoinAddress)scriptAddress);
		}
		public AddressEntry[] GetEntries(PubKey pubKey)
		{
			return GetEntries(pubKey.GetAddress(Configuration.Network));
		}
	}

	public class AddressEntry
	{
		internal class Entity : TableEntity
		{
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
