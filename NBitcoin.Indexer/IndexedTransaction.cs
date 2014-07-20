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
			var bytes = entities.Select(e => e.Transaction).FirstOrDefault(e => e != null);
			if(bytes != null)
			{
				Transaction tx = new Transaction();
				tx.ReadWrite(bytes);
				Transaction = tx;
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
	}
}
