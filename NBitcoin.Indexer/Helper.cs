using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace NBitcoin.Indexer
{
	internal static class Helper
	{
		internal static List<T> DeserializeList<T>(byte[] bytes) where T : IBitcoinSerializable, new()
		{
			List<T> outpoints = new List<T>();
			if(bytes == null)
				return outpoints;
			MemoryStream ms = new MemoryStream(bytes);
			ms.Position = 0;
			while(ms.Position != ms.Length)
			{
				T outpoint = new T();
				outpoint.ReadWrite(ms, false);
				outpoints.Add(outpoint);
			}
			return outpoints;
		}

		public static byte[] SerializeList<T>(IEnumerable<T> items) where T : IBitcoinSerializable
		{
			MemoryStream ms = new MemoryStream();
			foreach(var item in items)
			{
				item.ReadWrite(ms, true);
			}
			return Helper.GetBytes(ms) ?? new byte[0];
		}

		public static byte[] GetBytes(MemoryStream stream)
		{
			if(stream.Length == 0)
				return null;
			var buffer = stream.GetBuffer();
			Array.Resize(ref buffer, (int)stream.Length);
			if(buffer.Length > 1024 * 64)
				throw new ArgumentOutOfRangeException("stream", "Value too big to enter in an Azure Table Column");
			return buffer;
		}

		internal static byte[] Concat(params byte[][] arrays)
		{
			if(arrays.All(a => a == null))
				return null;
			if(arrays.Length == 1)
				return arrays[0];

			var lentgh = arrays.Where(a => a != null).Sum(a => a.Length);
			byte[] result = new byte[lentgh];
			var index = 0;
			foreach(var array in arrays.Where(a => a != null))
			{
				Array.Copy(array, 0, result, index, array.Length);
				index += array.Length;
			}
			return result;
		}

		internal static void Spread(byte[] value, int maxLength, ref byte[] a1, ref byte[] a2, ref byte[] a3, ref byte[] a4)
		{
			int index = 0;
			int array = 0;
			if(value == null)
			{
				a1 = null;
				a2 = null;
				a3 = null;
				a4 = null;
				return;
			}
			if(value.Length < maxLength)
			{
				a1 = value;
				a2 = null;
				a3 = null;
				a4 = null;
				return;
			}
			while(value.Length - index != 0 && array <= 3)
			{
				var toCopy = Math.Min(value.Length - index, maxLength);
				if(array == 0)
					a1 = CreateArray(value, index, toCopy);
				if(array == 1)
					a2 = CreateArray(value, index, toCopy);
				if(array == 2)
					a3 = CreateArray(value, index, toCopy);
				if(array == 3)
					a4 = CreateArray(value, index, toCopy);

				array++;
				index += toCopy;
			}
			while(array <= 3)
			{
				if(array == 0)
					a1 = null;
				if(array == 1)
					a2 = null;
				if(array == 2)
					a3 = null;
				if(array == 3)
					a4 = null;

				array++;
			}
		}

		private static byte[] CreateArray(byte[] array, int index, int len)
		{
			byte[] result = new byte[len];
			Array.Copy(array, index, result, 0, len);
			return result;
		}

		internal static void SetThrottling()
		{
			ServicePointManager.UseNagleAlgorithm = false;
			ServicePointManager.Expect100Continue = false;
			ServicePointManager.DefaultConnectionLimit = 100;
		}

		public static bool TryAdd<TKey, TValue>(this IDictionary<TKey, TValue> dictionary, TKey key, TValue value)
		{
			if(!dictionary.ContainsKey(key))
			{
				dictionary.Add(key, value);
				return true;
			}
			return false;
		}
	}
}
