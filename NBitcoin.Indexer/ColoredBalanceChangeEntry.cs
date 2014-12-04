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

        internal void AddReceivedCoin(Asset asset, Spendable receivedCoin, bool isTransfer)
        {
            _ReceivedCoins.Add(new ColoredCoin(asset, new Coin(receivedCoin.OutPoint, receivedCoin.TxOut)));
        }

        internal void AddSpentCoin(Asset asset, Spendable spentCoin)
        {
            _SentCoins.Add(new ColoredCoin(asset, new Coin(spentCoin.OutPoint, spentCoin.TxOut)));
        }

        public override string ToString()
        {
            return AssetId.GetWif(Network.Main) + " " + BalanceChange;
        }
    }
    public class ColoredBalanceChangeEntry
    {
        public ColoredBalanceChangeEntry(BalanceChangeEntry balanceChangeEntry, BalanceChangeEntry.Entity.ColorInformation colorInformation)
        {
            for (int i = 0 ; i < balanceChangeEntry.SpentCoins.Count ; i++)
            {
                var colorCoinInfo = colorInformation.Inputs[i];
                AddSpentCoin(colorCoinInfo.Asset, balanceChangeEntry.SpentCoins[i]);
            }
            for (int i = 0 ; i < balanceChangeEntry.ReceivedCoins.Count ; i++)
            {
                var colorCoinInfo = colorInformation.Outputs[i];
                AddReceivedCoin(colorCoinInfo.Asset, balanceChangeEntry.ReceivedCoins[i], colorCoinInfo.Transfer);
            }
        }

        public Money UncoloredBalanceChange
        {
            get
            {
                return UncoloredReceivedCoins.Select(c => c.TxOut.Value).Sum() - UncoloredSpentCoins.Select(c => c.TxOut.Value).Sum();
            }
        }

        private List<Spendable> _UncoloredReceivedCoins = new List<Spendable>();
        public List<Spendable> UncoloredReceivedCoins
        {
            get
            {
                return _UncoloredReceivedCoins;
            }
        }
        private List<Spendable> _UncoloredSpentCoins = new List<Spendable>();
        public List<Spendable> UncoloredSpentCoins
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


        private void AddReceivedCoin(Asset asset, Spendable receivedCoin, bool isTransfer)
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
        private void AddSpentCoin(Asset asset, Spendable spentCoin)
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
