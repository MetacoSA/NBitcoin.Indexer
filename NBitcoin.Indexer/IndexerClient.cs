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
			var table = Configuration.GetTransactionTable();
			var result = new IndexedTransaction[txIds.Length];
			var queries = new TableQuery<TransactionEntity>[txIds.Length];
			try
			{
				Parallel.For(0, txIds.Length, i =>
				{
					queries[i] = new TableQuery<TransactionEntity>()
									.Where(
											TableQuery.CombineFilters(
												TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, TransactionEntity.GetPartitionKey(txIds[i])),
												TableOperators.And,
												TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.GreaterThan, txIds[i].ToString())
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
	}
}
