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


		public IndexedTransaction GetTransaction(uint256 txId)
		{
			return GetTransactions(txId).First();
		}

		/// <summary>
		/// Get transactions in Azure Table
		/// </summary>
		/// <param name="txIds"></param>
		/// <returns>All transactions (with null entries for unfound transactions)</returns>
		public IndexedTransaction[] GetTransactions(params uint256[] txIds)
		{
			var result = new IndexedTransaction[txIds.Length];
			var queries = new TableQuery<TransactionEntity>[txIds.Length];
			try
			{
				Parallel.For(0, txIds.Length, i =>
				{
					var table = Configuration.GetTransactionTable();
					queries[i] = new TableQuery<TransactionEntity>()
									.Where(
											TableQuery.CombineFilters(
												TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, TransactionEntity.GetPartitionKey(txIds[i])),
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
						result[i] = new IndexedTransaction(entities);
						if(result[i].Transaction == null)
						{
							foreach(var blockId in result[i].BlockIds)
							{
								var block = GetBlock(blockId);
								if(block != null)
								{
									result[i].Transaction = block.Transactions.FirstOrDefault(t => t.GetHash() == txIds[i]);
									entities[0].Transaction = result[i].Transaction.ToBytes();
									if(entities[0].Transaction.Length < 1024 * 64)
										table.Execute(TableOperation.Merge(entities[0]));
									break;
								}
							}
							if(result[i].Transaction == null)
								result[i] = null;
						}
					}
				});
			}
			catch(AggregateException ex)
			{
				throw ex.InnerException;
			}
			return result;
		}


		public AddressEntry[] GetEntries(BitcoinAddress address)
		{
			var addressStr = address.ToString();
			var table = Configuration.GetBalanceTable();
			var query = new TableQuery<IndexedAddressEntry>()
							.Where(
							TableQuery.CombineFilters(
												TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, IndexedAddressEntry.GetPartitionKey(addressStr)),
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
				if(indexEntry.Money == null)
					if(LazyLoad(indexEntry))
					{
						table.Execute(TableOperation.Merge(indexEntry));
					}
				entry.BalanceChange = indexEntry.Money == null ? (Money)null : Money.Parse(indexEntry.Money);
				entry.BlockIds = indexEntryGroup
										.Where(s => s.BlockId != String.Empty)
										.Select(s => new uint256(s.BlockId)).ToArray();
				entry.Received = indexEntry.GetReceivedOutput();
				result.Add(entry);
			}
			return result.ToArray();
		}

		private bool LazyLoad(IndexedAddressEntry indexAddress)
		{
			var txId = new uint256(indexAddress.TransactionId);
			var indexedTx = GetTransaction(txId);
			if(indexedTx == null)
				return false;

			Money total = Money.Zero;

			var received = indexAddress.GetReceivedOutput();
			total =
				total +
				indexedTx.Transaction.Outputs.Where((o, i) => received.Contains(i))
				.Select(o => o.Value)
				.Sum();


			Dictionary<uint256, Transaction> transactionsCache = new Dictionary<uint256, Transaction>();
			transactionsCache.Add(indexedTx.Transaction.GetHash(), indexedTx.Transaction);

			var sentOutputs = indexAddress.GetSentOutpoints();

			foreach(var sent in sentOutputs)
			{
				Transaction sourceTransaction = null;
				if(!transactionsCache.TryGetValue(sent.Hash, out sourceTransaction))
				{
					var sourceIndexedTx = GetTransaction(sent.Hash);
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
				var sourceOutput = sourceTransaction.Outputs[(int)sent.N];
				total = total - sourceOutput.Value;
			}


			indexAddress.Money = total.ToString();
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

		public List<OutPoint> Spent
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

		public List<int> Received
		{
			get;
			set;
		}

		public override string ToString()
		{
			return Address + " - " + (BalanceChange == null ? "??" : BalanceChange.ToString());
		}
	}
}
