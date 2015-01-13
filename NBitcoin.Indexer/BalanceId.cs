using NBitcoin.Indexer.DamienG.Security.Cryptography;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NBitcoin.Indexer
{
    public enum BalanceType
    {
        Wallet,
        Address
    }
    public class BalanceId
    {
        const string WalletPrefix = "w$";
        internal const int MaxScriptSize = 512;
        public BalanceId(string walletId)
        {
            _Internal = WalletPrefix + FastEncoder.Instance.EncodeData(Encoding.UTF8.GetBytes(walletId));
        }
        public BalanceId(Script scriptPubKey)
        {
            var pubKey = scriptPubKey.ToBytes(true);
            if (pubKey.Length > MaxScriptSize)
                _Internal = FastEncoder.Instance.EncodeData(scriptPubKey.Hash.ToBytes(true));
            else
                _Internal = FastEncoder.Instance.EncodeData(scriptPubKey.ToBytes(true));
        }
        public BalanceId(IDestination destination)
            : this(destination.ScriptPubKey)
        {
        }

        private BalanceId()
        {

        }


        public BalanceType Type
        {
            get
            {
                return _Internal.StartsWith(WalletPrefix) ? BalanceType.Wallet : BalanceType.Address;
            }
        }

        public string PartitionKey
        {
            get
            {
                if (_PartitionKey == null)
                {
                    _PartitionKey = Helper.GetPartitionKey(10, Crc32.Compute(_Internal));
                }
                return _PartitionKey;
            }
        }

        public Script ExtractScript()
        {
            if (_Internal.StartsWith(WalletPrefix))
                return null;
            return Script.FromBytesUnsafe(FastEncoder.Instance.DecodeData(_Internal));
        }


        string _PartitionKey;
        string _Internal;
        public override string ToString()
        {
            return _Internal;
        }

        public static BalanceId Parse(string balanceId)
        {
            return new BalanceId()
            {
                _Internal = balanceId
            };
        }
    }
}
