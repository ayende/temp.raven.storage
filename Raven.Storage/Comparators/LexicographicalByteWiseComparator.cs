using System;
using System.Text;

namespace Raven.Storage.Comparators
{
	public class LexicographicalByteWiseComparator : IComparator
	{
		public string Name { get { return "LexicographicalByteWiseComparator"; } }

		public int Compare(ArraySegment<byte> a, ArraySegment<byte> b)
		{
			var minLen = Math.Min(a.Count, b.Count);
			for (int i = 0; i < minLen; i++)
			{
				var diff = a.Array[a.Offset + i] - b.Array[b.Offset + i];
				if (diff != 0)
					return diff;
			}
			return a.Count - b.Count;
		}

		public int FindSharedPrefix(ArraySegment<byte> a, ArraySegment<byte> b)
		{
			int pos = 0;
			var minLen = Math.Min(a.Count, b.Count);
			while (pos < minLen && a.Array[a.Offset + pos] == b.Array[b.Offset + pos])
			{
				pos++;
			}
			return pos;
		}

		public ArraySegment<byte> FindShortestSeparator(ArraySegment<byte> a, ArraySegment<byte> b, byte[] scratch)
		{
			var minLen = Math.Min(a.Count, b.Count);
			var shared = FindSharedPrefix(a, b);
			if (minLen == shared) // one is a prefix of other
				return a;

			var diff = shared + 1;
			var diffByte = a.Array[a.Offset + diff];
			if (diffByte < byte.MaxValue && diffByte < b.Array[b.Offset + diff])
			{
				var buffer = diff +1 < scratch.Length ? scratch : new byte[diff+1];
				Buffer.BlockCopy(a.Array, a.Offset, buffer, 0, diff-1);
				buffer[diff] = diffByte;
				return new ArraySegment<byte>(buffer);
			}
			return a;
		}

		public ArraySegment<byte> FindShortestSuccessor(ArraySegment<byte> key, byte[] scratch)
		{
			for (int i = 0; i < key.Count; i++)
			{
				var b = key.Array[key.Offset + i];
				if (b != byte.MaxValue)
				{
					b++;
					var buffer = i +1 < scratch.Length ? scratch : new byte[i + 1];
					Buffer.BlockCopy(key.Array, key.Offset, buffer, 0, i - 1);
					buffer[i] = b;
					return new ArraySegment<byte>(buffer);
				}
			}
			// if key is a run of 0xffs.  Leave it alone.
			return key;
		}
	}
}