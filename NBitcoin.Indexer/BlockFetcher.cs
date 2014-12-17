using NBitcoin.Protocol;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NBitcoin.Indexer
{
    public class BlockInfo
    {
        public int Height
        {
            get;
            set;
        }
        public uint256 BlockId
        {
            get;
            set;
        }
        public Block Block
        {
            get;
            set;
        }
    }
    public class BlockFetcher : IEnumerable<BlockInfo>
    {

        private readonly Checkpoint _Checkpoint;
        public Checkpoint Checkpoint
        {
            get
            {
                return _Checkpoint;
            }
        }
        private readonly Node _Node;
        public Node Node
        {
            get
            {
                return _Node;
            }
        }

        private readonly ChainBase _BlockHeaders;
        public ChainBase BlockHeaders
        {
            get
            {
                return _BlockHeaders;
            }
        }
        public BlockFetcher(Checkpoint checkpoint, Node node, ChainBase blockHeaders)
        {
            if (node == null)
                throw new ArgumentNullException("node");
            if (blockHeaders == null)
                throw new ArgumentNullException("blockHeaders");
            if (checkpoint == null)
                throw new ArgumentNullException("checkpoint");
            CheckpointInterval = TimeSpan.FromMinutes(15);
            _BlockHeaders = blockHeaders;
            _Node = node;
            _Checkpoint = checkpoint;
        }

        public bool DisableSaving
        {
            get;
            set;
        }

        #region IEnumerable<BlockInfo> Members

        ChainedBlock _LastProcessed;
        public IEnumerator<BlockInfo> GetEnumerator()
        {
            var fork = _BlockHeaders.FindFork(_Checkpoint.BlockLocator);
            var headers = _BlockHeaders.EnumerateAfter(fork);
            headers = headers.Where(h => h.Height >= FromHeight && h.Height <= ToHeight);
            var first = headers.FirstOrDefault();
            if (first == null)
                yield break;
            var height = first.Height;
            if (first.Height == 1)
            {
                headers = new[] { fork }.Concat(headers);
                height = 0;
            }
           
            foreach (var block in _Node.GetBlocks(headers.Select(b => b.HashBlock)))
            {
                var header = _BlockHeaders.GetBlock(height);
                _LastProcessed = header;
                yield return new BlockInfo()
                {
                    Block = block,
                    BlockId = header.HashBlock,
                    Height = header.Height
                };
                IndexerTrace.Processed(height, _BlockHeaders.Tip.Height);
                height++;
            }
        }

        #endregion

        #region IEnumerable Members

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        #endregion

        public TimeSpan CheckpointInterval
        {
            get;
            set;
        }

        private DateTime _LastSaved = DateTime.UtcNow;
        public bool NeedSave
        {
            get
            {
                return (DateTime.UtcNow - _LastSaved) > CheckpointInterval && !DisableSaving;
            }
        }

        public void SaveCheckpoint()
        {
            if (DisableSaving || _LastProcessed == null)
                return;

            _Checkpoint.SaveProgress(_LastProcessed);
            IndexerTrace.CheckpointSaved(_LastProcessed, _Checkpoint.FileName);

            if (NeedSave)
            {
                _LastSaved = DateTime.Now;
            }
        }

        public int FromHeight
        {
            get;
            set;
        }

        public int ToHeight
        {
            get;
            set;
        }
    }
}
