using Microsoft.WindowsAzure.Storage.Table;
using NBitcoin.DataEncoders;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NBitcoin.Indexer
{
	public class IndexedAddress : TableEntity
	{
		public IndexedAddress(BitcoinAddress address,
							  Script scriptPubKey,
							  OutPoint spendable,
							  Money value,
							  uint256 blockId)
		{
			Value = value.ToString();
			var wif = address.ToString();
			PartitionKey = GetPartitionKey(wif);
			RowKey =
				string.Format("{0}-{1}-r-{2}",
						wif
						, Encoders.Hex.EncodeData(spendable.ToBytes())
						, Encoders.Hex.EncodeData(blockId.ToBytes()));
		}

		private string GetPartitionKey(string wif)
		{
			return new string(new char[] { wif[wif.Length - 2], wif[wif.Length - 1] });
		}

		public IndexedAddress(BitcoinAddress address,
								Script scriptSig,
								OutPoint spent,
								uint256 spentLocation,
								uint256 blockId)
		{
			var wif = address.ToString();
			PartitionKey = GetPartitionKey(wif);
			RowKey =
				string.Format("{0}-{1}-s-{2}-{3}",
						wif
						, Encoders.Hex.EncodeData(spent.ToBytes())
						, Encoders.Hex.EncodeData(blockId.ToBytes())
						, Encoders.Hex.EncodeData(spentLocation.ToBytes()));
		}

		
		public string Value
		{
			get;
			set;
		}


	}
}
