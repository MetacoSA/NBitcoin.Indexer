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

		public TransactionEntry GetTransaction(bool lazyLoadSpentOutput, uint256 txId)
		{
			return GetTransactions(lazyLoadSpentOutput, new uint256[] { txId }).First();
		}
		public TransactionEntry GetTransaction(uint256 txId)
		{
			return GetTransactions(true, new[] { txId }).First();
		}

		/// <summary>
		/// Get transactions in Azure Table
		/// </summary>
		/// <param name="txIds"></param>
		/// <returns>All transactions (with null entries for unfound transactions)</returns>
		public TransactionEntry[] GetTransactions(bool lazyLoadSpentOutput, uint256[] txIds)
		{
			var result = new TransactionEntry[txIds.Length];
			var queries = new TableQuery<TransactionEntry.Entity>[txIds.Length];
			try
			{
				Parallel.For(0, txIds.Length, i =>
				{
					var table = Configuration.GetTransactionTable();
					queries[i] = new TableQuery<TransactionEntry.Entity>()
									.Where(
											TableQuery.CombineFilters(
												TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, TransactionEntry.Entity.GetPartitionKey(txIds[i])),
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
						result[i] = new TransactionEntry(entities);
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

			var entitiesByTransactionId = table
									.ExecuteQuery(query)
									.GroupBy(e => e.TransactionId);
			List<AddressEntry> result = new List<AddressEntry>();
			foreach(var entities in entitiesByTransactionId)
			{
				var entity = entities.Where(e=>e.Loaded).FirstOrDefault();
				if(entity == null)
					entity = entities.First();
				if(!entity.Loaded)
					if(LoadAddressEntity(entity))
					{
						table.Execute(TableOperation.Merge(entity));
					}
				var entry = new AddressEntry(entity, entities.ToArray());
				result.Add(entry);
			}
			return result.ToArray();
		}

		public AddressEntry[][] GetAllEntries(BitcoinAddress[] addresses)
		{
			Helper.SetThrottling();
			AddressEntry[][] result = new AddressEntry[addresses.Length][];
			Parallel.For(0, addresses.Length,
			i =>
			{
				result[i] = GetEntries(addresses[i]);
			});
			return result;
		}

		public bool LoadAddressEntity(AddressEntry.Entity indexAddress)
		{
			return LoadAddressEntity(indexAddress, null);
		}
		public bool LoadAddressEntity(AddressEntry.Entity indexAddress, IDictionary<uint256, Transaction> transactionsCache)
		{
			if(transactionsCache == null)
				transactionsCache = new Dictionary<uint256, Transaction>();
			var txId = new uint256(indexAddress.TransactionId);

			Transaction tx = null;
			if(!transactionsCache.TryGetValue(txId, out tx))
			{
				var indexed = GetTransaction(txId);
				if(indexed != null)
					tx = indexed.Transaction;
			}
			if(tx == null)
				return false;

			Money total = Money.Zero;

			var received = indexAddress.GetReceivedOutput();
			indexAddress.ReceivedTxOuts =
							Helper.SerializeList(tx.Outputs.Where((o, i) => received.Contains(i))
							.ToList());


			transactionsCache.AddOrReplace(txId, tx);

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
						transactionsCache.AddOrReplace(sent.Hash, sourceTransaction);
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
}
