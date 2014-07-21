using Microsoft.WindowsAzure.Storage.Table;
using NBitcoin.DataEncoders;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NBitcoin.Indexer
{
	internal class IndexedAddressEntry : TableEntity
	{
		public const int MaxOutpoint = 100;

		public IndexedAddressEntry()
		{

		}
		public IndexedAddressEntry(TxDestination reciever, OutPoint[] spendable)
		{
			SetAccount(reciever);
			SetOutpoint(spendable);
		}

		private void SetOutpoint(OutPoint[] spendables)
		{
			if(spendables.Length > 100) //Prevent any column of AzureTable to go more than 64k
				throw new ArgumentOutOfRangeException("spendables", "You can save at most 100 outpoints in an entry");

			StringBuilder builder = new StringBuilder();
			foreach(var spendable in spendables)
			{
				builder.Append(spendable.N + ":" + spendable.Hash + ";");
			}
			OutPoints = builder.ToString();
		}

		private void SetAccount(TxDestination account)
		{
			var suffix = account is KeyId ? "a" : "s";
			var accountStr = account.ToString();
			PartitionKey = accountStr.Substring(3);
			RowKey = accountStr + "-" + suffix + "-" + Encoders.Hex.EncodeData(RandomUtils.GetBytes(10));
		}
		public IndexedAddressEntry(uint256 txId, TxDestination sender, OutPoint[] spent)
		{
			SetAccount(sender);
			SetOutpoint(spent);
			SpendingTx = txId.ToString();
		}

		public string SpendingTx
		{
			get;
			set;
		}

		public string OutPoints
		{
			get;
			set;
		}
	}
}
