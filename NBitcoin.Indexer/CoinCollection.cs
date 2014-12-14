using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NBitcoin.Indexer
{
    public class CoinCollection : List<Coin>
    {
        public CoinCollection()
        {

        }
        public CoinCollection(IEnumerable<Coin> enumerable)
        {
            AddRange(enumerable);
        }
        public Coin this[OutPoint index]
        {
            get
            {
                for (int i = 0 ; i < this.Count ; i++)
                {
                    if (this[i].Outpoint == index)
                        return this[i];
                }
                throw new KeyNotFoundException();
            }
            set
            {
                for (int i = 0 ; i < this.Count ; i++)
                {
                    if (this[i].Outpoint == index)
                        this[i] = value;
                }
                throw new KeyNotFoundException();
            }
        }
    }
}
