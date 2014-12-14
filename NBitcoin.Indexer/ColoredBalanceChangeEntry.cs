using NBitcoin.OpenAsset;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NBitcoin.Indexer
{
    public class AssetBalanceChange
    {
        public AssetBalanceChange(AssetId id)
        {
            AssetId = id;
        }

        public AssetId AssetId
        {
            get;
            set;
        }

        public long BalanceChange
        {
            get
            {
                return _ReceivedCoins.Select(r => r.Amount).Sum() - _SentCoins.Select(r => r.Amount).Sum();
            }
        }


        private List<ColoredCoin> _ReceivedCoins = new List<ColoredCoin>();
        public List<ColoredCoin> ReceivedCoins
        {
            get
            {
                return _ReceivedCoins;
            }
        }

        private List<ColoredCoin> _SentCoins = new List<ColoredCoin>();
        public List<ColoredCoin> SentCoins
        {
            get
            {
                return _SentCoins;
            }
        }

        internal void AddReceivedCoin(Asset asset, Coin receivedCoin, bool isTransfer)
        {
            _ReceivedCoins.Add(new ColoredCoin(asset, receivedCoin));
        }

        internal void AddSpentCoin(Asset asset, Coin spentCoin)
        {
            _SentCoins.Add(new ColoredCoin(asset, spentCoin));
        }

        public override string ToString()
        {
            return AssetId.GetWif(Network.Main) + " " + BalanceChange;
        }
    }
    public class ColoredBalanceChangeEntry
    {
        internal ColoredTransaction _Colored;
        public ColoredBalanceChangeEntry(OrderedBalanceChange balanceChange, ColoredTransaction coloredTransaction)
        {
            _Colored = coloredTransaction;
            for (var i = 0 ; i < balanceChange.SpentIndices.Count ; i++)
            {
                var spentIndex = balanceChange.SpentIndices[i];
                var entry = coloredTransaction.Inputs.FirstOrDefault(o => o.Index == (uint)spentIndex);
                if (entry != null)
                    AddSpentCoin(entry.Asset, balanceChange.SpentCoins[(int)i]);
                else
                    AddSpentCoin(null, balanceChange.SpentCoins[(int)i]);
            }
            foreach (var coin in balanceChange.ReceivedCoins)
            {
                var entry = coloredTransaction.GetColoredEntry(coin.Outpoint.N);
                if (entry != null)
                    AddReceivedCoin(entry.Asset, coin, !coloredTransaction.Issuances.Contains(entry));
                else
                    AddReceivedCoin(null, coin, false);
            }
        }

        public Money UncoloredBalanceChange
        {
            get
            {
                return UncoloredReceivedCoins.Select(c => c.TxOut.Value).Sum() - UncoloredSpentCoins.Select(c => c.TxOut.Value).Sum();
            }
        }

        private List<Coin> _UncoloredReceivedCoins = new List<Coin>();
        public List<Coin> UncoloredReceivedCoins
        {
            get
            {
                return _UncoloredReceivedCoins;
            }
        }
        private List<Coin> _UncoloredSpentCoins = new List<Coin>();
        public List<Coin> UncoloredSpentCoins
        {
            get
            {
                return _UncoloredSpentCoins;
            }
        }

        Dictionary<AssetId, AssetBalanceChange> _ColoredAssets = new Dictionary<AssetId, AssetBalanceChange>();


        private AssetBalanceChange GetAsset(AssetId assetId, bool createIfNotExists)
        {
            AssetBalanceChange change = null;
            if (_ColoredAssets.TryGetValue(assetId, out change))
                return change;
            if (createIfNotExists)
            {
                change = new AssetBalanceChange(assetId);
                _ColoredAssets.Add(assetId, change);
            }
            return change;
        }

        public AssetBalanceChange GetAsset(BitcoinAssetId asset)
        {
            return GetAsset(asset.AssetId);
        }
        public AssetBalanceChange GetAsset(AssetId assetId)
        {
            return GetAsset(assetId, false);
        }


        private void AddReceivedCoin(Asset asset, Coin receivedCoin, bool isTransfer)
        {
            if (asset == null)
            {
                _UncoloredReceivedCoins.Add(receivedCoin);
            }
            else
            {
                GetAsset(asset.Id, true).AddReceivedCoin(asset, receivedCoin, isTransfer);
            }
        }
        private void AddSpentCoin(Asset asset, Coin spentCoin)
        {
            if (asset == null)
            {
                _UncoloredSpentCoins.Add(spentCoin);
            }
            else
            {
                GetAsset(asset.Id, true).AddSpentCoin(asset, spentCoin);
            }
        }

        public override string ToString()
        {
            return "Uncolored balance : " + UncoloredBalanceChange.ToString();
        }
    }
}
