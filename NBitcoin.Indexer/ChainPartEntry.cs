using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NBitcoin.Indexer
{
    public class ChainPartEntry
    {
        public ChainPartEntry()
        {
            BlockHeaders = new List<BlockHeader>();
        }

        public ChainPartEntry(DynamicTableEntity entity)
        {
            ChainOffset = StringToHeight(entity.RowKey);
            BlockHeaders = new List<BlockHeader>();         
            foreach (var prop in entity.Properties)
            {
                var header = new BlockHeader();
                header.FromBytes(prop.Value.BinaryValue);
                BlockHeaders.Add(header);
            }
        }

        public int ChainOffset
        {
            get;
            set;
        }

        public List<BlockHeader> BlockHeaders
        {
            get;
            private set;
        }

        public BlockHeader GetHeader(int height)
        {
            if (height < ChainOffset)
                return null;
            height = height - ChainOffset;
            if (height >= BlockHeaders.Count)
                return null;
            return BlockHeaders[height];
        }

        public DynamicTableEntity ToEntity()
        {
            DynamicTableEntity entity = new DynamicTableEntity();
            entity.PartitionKey = "a";
            entity.RowKey = HeightToString(ChainOffset);
            int i = 0;
            foreach (var header in BlockHeaders)
            {
                entity.Properties.Add("a" + i, new EntityProperty(header.ToBytes()));
                i++;
            }
            return entity;
        }

        static string format = new string(Enumerable.Range(0, int.MaxValue.ToString().Length).Select(c => '0').ToArray());
        static char[] Digit = Enumerable.Range(0, 10).Select(c => c.ToString()[0]).ToArray();
        static char[] InvertDigit = Enumerable.Range(0, 10).Reverse().Select(c => c.ToString()[0]).ToArray();

        //Convert '012' to '987'
        private static string HeightToString(int height)
        {
            var input = height.ToString(format);
            char[] result = new char[format.Length];
            for (int i = 0 ; i < result.Length ; i++)
            {
                var index = Array.IndexOf(Digit, input[i]);
                result[i] = InvertDigit[index];
            }
            return new string(result);
        }

        //Convert '987' to '012'
        private int StringToHeight(string rowkey)
        {
            char[] result = new char[format.Length];
            for (int i = 0 ; i < result.Length ; i++)
            {
                var index = Array.IndexOf(InvertDigit, rowkey[i]);
                result[i] = Digit[index];
            }
            return int.Parse(new string(result));
        }
    }
}
