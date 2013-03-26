using System;
using System.Text;
using Raven.Storage.Data;

namespace Raven.Storage.Comparators
{
	public class ByteWiseComparator : IComparator
	{
		public readonly static ByteWiseComparator  Default = new ByteWiseComparator();

		public string Name { get { return "ByteWiseComparator"; } }

		public int Compare(Slice a, Slice b)
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

		public int FindSharedPrefix(Slice a, Slice b)
		{
			int pos = 0;
			var minLen = Math.Min(a.Count, b.Count);
			while (pos < minLen && a.Array[a.Offset + pos] == b.Array[b.Offset + pos])
			{
				pos++;
			}
			return pos;
		}

		public Slice FindShortestSeparator(Slice a, Slice b, ref byte[] scratch)
		{
			var minLen = Math.Min(a.Count, b.Count);
			var shared = FindSharedPrefix(a, b);
			if (minLen == shared) // one is a prefix of other
				return a;

			var diff = shared + 1;
			var diffByte = a.Array[a.Offset + diff];
			if (diffByte < byte.MaxValue && diffByte < b.Array[b.Offset + diff])
			{
				scratch  = diff + 1 < scratch.Length ? scratch : new byte[diff + 1];
				if(diff > 0)
					Buffer.BlockCopy(a.Array, a.Offset, scratch, 0, diff - 1);
				scratch[diff] = diffByte;
				return new Slice(scratch, a.Offset, diff + 1);
			}
			return a;
		}

		public Slice FindShortestSuccessor(Slice key, ref byte[] scratch)
		{
			for (int i = 0; i < key.Count; i++)
			{
				var b = key.Array[key.Offset + i];
				if (b != byte.MaxValue)
				{
					b++;
					scratch = i + 1 < scratch.Length ? scratch : new byte[i + 1];
					if(i > 0)
						Buffer.BlockCopy(key.Array, key.Offset, scratch, 0, i - 1);
					scratch[i] = b;
					return new Slice(scratch, key.Offset, i+1);
				}
			}
			// if key is a run of 0xffs.  Leave it alone.
			return key;
		}
	}
}