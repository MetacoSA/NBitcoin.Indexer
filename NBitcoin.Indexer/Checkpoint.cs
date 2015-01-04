using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NBitcoin.Indexer
{
    public class Checkpoint
    {
        string _FileName;
        public string FileName
        {
            get
            {
                return _FileName;
            }
        }

        public Checkpoint(string fileName, Network network)
        {
            _FileName = fileName;
            try
            {

                _BlockLocator = new NBitcoin.BlockLocator();
                _BlockLocator.FromBytes(File.ReadAllBytes(fileName));
            }
            catch
            {
                var list = new List<uint256>();
                list.Add(network.GetGenesis().Header.GetHash());
                _BlockLocator = new BlockLocator(list);
            }
        }


        public uint256 Genesis
        {
            get
            {
                return BlockLocator.Blocks[BlockLocator.Blocks.Count - 1];
            }
        }

        BlockLocator _BlockLocator;
        public BlockLocator BlockLocator
        {
            get
            {
                return _BlockLocator;
            }
        }

        public void SaveProgress(ChainedBlock tip)
        {
            SaveProgress(tip.GetLocator());
        }
        public void SaveProgress(BlockLocator locator)
        {
            _BlockLocator = locator;
            File.WriteAllBytes(_FileName, _BlockLocator.ToBytes());
        }
    }

}
