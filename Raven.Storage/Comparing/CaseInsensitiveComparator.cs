using System;
using Raven.Storage.Data;

namespace Raven.Storage.Comparing
{
	using System.Runtime.CompilerServices;

	public class CaseInsensitiveComparator : IComparator
	{
		public static readonly CaseInsensitiveComparator Default = new CaseInsensitiveComparator();

		public string Name { get { return "CaseInsensitiveComparator"; } }

		/// <summary>
		/// Note that this probably won't do very well for case insensitive chars that uses
		/// more than a single byte. That is good enough for now.
		/// </summary>
		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public int Compare(Slice a, Slice b)
		{
			var minLen = Math.Min(a.Count, b.Count);
			for (int i = 0; i < minLen; i++)
			{
				var cha = (char)a.Array[a.Offset + i];
				var chb = (char)b.Array[b.Offset + i];

				if (((cha >= 'a' && cha <= 'z') || (cha >= 'A' && cha <= 'Z')) && ((chb >= 'a' && chb <= 'z') || (chb >= 'A' && chb <= 'Z')))
				{
					var diff = cha - chb;
					if (diff == 0 || diff == 32 || diff == -32)
						continue;

					return diff;
				}
				else
				{
					var diff = cha - chb;
					if (diff != 0)
						return diff;
				}
			}

			return a.Count - b.Count;
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public int FindSharedPrefix(Slice a, Slice b)
		{
			return ByteWiseComparator.Default.FindSharedPrefix(a, b);
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public void FindShortestSeparator(ref Slice start, Slice limit)
		{
			ByteWiseComparator.Default.FindShortestSeparator(ref start, limit);
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public Slice FindShortestSuccessor(Slice key, ref byte[] scratch)
		{
			return ByteWiseComparator.Default.FindShortestSuccessor(key, ref scratch);
		}
	}
}