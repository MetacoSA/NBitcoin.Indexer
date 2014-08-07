using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NBitcoin.Indexer
{
	public class IndexedTransaction
	{
		internal IndexedTransaction(TransactionEntity[] entities)
		{
			List<uint256> blockIds = new List<uint256>();
			foreach(var entity in entities)
			{
				var parts = entity.RowKey.Split(new string[] { "-b" }, StringSplitOptions.RemoveEmptyEntries);
				if(parts.Length != 2)
					throw new FormatException("Invalid TransactionEntity");
				if(TransactionId == null)
					TransactionId = new uint256(parts[0]);
				blockIds.Add(new uint256(parts[1]));
			}
			BlockIds = blockIds.ToArray();
			var loadedEntity = entities.Where(e => e.Transaction != null).FirstOrDefault();
			if(loadedEntity != null)
			{
				var bytes = loadedEntity.Transaction;
				Transaction tx = new Transaction();
				tx.ReadWrite(bytes);
				Transaction = tx;

				if(loadedEntity.SpentOutputs != null)
				{
					SpentTxOuts = Helper.DeserializeList<TxOut>(loadedEntity.SpentOutputs).ToArray();
				}
			}

		}

		public Money Fees
		{
			get
			{
				if(SpentTxOuts == null || Transaction == null)
					return null;
				return SpentTxOuts.Select(o => o.Value).Sum() - Transaction.TotalOut;
			}
		}
		public uint256[] BlockIds
		{
			get;
			internal set;
		}
		public uint256 TransactionId
		{
			get;
			internal set;
		}
		public Transaction Transaction
		{
			get;
			internal set;
		}

		public TxOut[] SpentTxOuts
		{
			get;
			set;
		}
	}
}
