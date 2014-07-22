using Microsoft.WindowsAzure.Storage.Table;
using NBitcoin.Crypto;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NBitcoin.Indexer
{
	class TransactionEntity : TableEntity
	{
		public TransactionEntity()
		{

		}

		public TransactionEntity(Transaction tx)
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
		public TransactionEntity(Transaction tx, uint256 blockId)
		{
			SetTx(tx);
			RowKey = _txId.ToString() + "-b" + blockId.ToString();
		}

		public byte[] Transaction
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
}
