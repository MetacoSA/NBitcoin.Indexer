using Microsoft.WindowsAzure.Storage.Table;
using NBitcoin.Crypto;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NBitcoin.Indexer
{
	public class IndexedTransaction : TableEntity
	{
		public IndexedTransaction()
		{

		}

		public IndexedTransaction(Transaction tx)
		{
			SetTx(tx);
			RowKey = _txId.ToString() + "-m";
		}

		private void SetTx(Transaction tx)
		{
			var transaction = tx.ToBytes();
			_txId = Hashes.Hash256(transaction);
			if(transaction.Length < 1024 * 64)
				Transaction = transaction;
			Key = (ushort)((_txId.GetByte(0) & 0xE0) + (_txId.GetByte(1) << 8));
		}
		public IndexedTransaction(Transaction tx, uint256 blockId)
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
	}
}
