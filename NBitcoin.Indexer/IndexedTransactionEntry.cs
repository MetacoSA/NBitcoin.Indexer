using Microsoft.WindowsAzure.Storage.Table;
using NBitcoin.Crypto;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NBitcoin.Indexer
{
	public class IndexedTransactionEntry
	{
		internal class Entity : TableEntity
		{
			public Entity()
			{

			}

			public Entity(Transaction tx)
			{
				SetTx(tx);
				RowKey = _txId.ToString() + "-m";
			}

			private void SetTx(Transaction tx)
			{
				var transaction = tx.ToBytes();
				_txId = Hashes.Hash256(transaction);
				//if(transaction.Length < 1024 * 64)
				//	Transaction = transaction;
				Key = GetPartitionKeyUShort(_txId);
			}
			public Entity(Transaction tx, uint256 blockId)
			{
				SetTx(tx);
				RowKey = _txId.ToString() + "-b" + blockId.ToString();
			}

			public byte[] Transaction
			{
				get;
				set;
			}

			public byte[] SpentOutputs
			{
				get;
				set;
			}

			uint256 _txId;
			ushort? _Key;
			[IgnoreProperty]
			public ushort Key
			{
				get
				{
					if(_Key == null)
						_Key = ushort.Parse(PartitionKey, System.Globalization.NumberStyles.HexNumber);
					return _Key.Value;
				}
				set
				{
					PartitionKey = value.ToString("X2");
					_Key = value;
				}
			}
			public static string GetPartitionKey(uint256 txid)
			{
				var id = GetPartitionKeyUShort(txid);
				return id.ToString("X2");
			}

			private static ushort GetPartitionKeyUShort(uint256 txid)
			{
				return (ushort)((txid.GetByte(0) & 0xE0) + (txid.GetByte(1) << 8));
			}
			public override string ToString()
			{
				return PartitionKey + " " + RowKey;
			}

		}
		internal IndexedTransactionEntry(Entity[] entities)
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
