using System;
using System.Diagnostics;
using System.Runtime.InteropServices;
using Raven.Storage.Data;

namespace Raven.Storage.Comparing
{
	using System.Runtime.CompilerServices;

	public class ByteWiseComparator : IComparator
	{
		public readonly static ByteWiseComparator  Default = new ByteWiseComparator();

		public string Name { get { return "ByteWiseComparator"; } }

		[DllImport("msvcrt.dll", CallingConvention = CallingConvention.Cdecl)]
		static extern int memcmp(byte[] b1, byte[] b2, int count);

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public int Compare(Slice a, Slice b)
		{
			var minLen = Math.Min(a.Count, b.Count);
			var result = memcmp(a.Array, b.Array, minLen);
			return result == 0 ? a.Count - b.Count : result;
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
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

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public void FindShortestSeparator(ref Slice start, Slice limit)
		{
			var minLen = Math.Min(start.Count, limit.Count);
			var shared = FindSharedPrefix(start, limit);
			if (minLen == shared) // one is a prefix of other
				return;

			var diff = shared + 1;
			var diffByte = start.Array[start.Offset + diff];
			if (diffByte < byte.MaxValue && diffByte < limit.Array[limit.Offset + diff])
			{
				start.Array[diff] ++;
				start = new Slice(start.Array, start.Offset, diff + 1);
				Debug.Assert(Compare(start, limit) < 0);
			}
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
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