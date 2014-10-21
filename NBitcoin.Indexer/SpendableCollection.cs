using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NBitcoin.Indexer
{
    public class SpendableCollection : List<Spendable>
    {
        public Spendable this[OutPoint index]
        {
            get
            {
                for (int i = 0 ; i < this.Count ; i++)
                {
                    if (this[i].OutPoint == index)
                        return this[i];
                }
                throw new KeyNotFoundException();
            }
            set
            {
                for (int i = 0 ; i < this.Count ; i++)
                {
                    if (this[i].OutPoint == index)
                        this[i] = value;
                }
                throw new KeyNotFoundException();
            }
        }
    }
}
