using System;

namespace Raven.Storage.Comparators
{
	public class CaseInsensitiveComparator : IComparator
	{
		readonly LexicographicalByteWiseComparator inner = new LexicographicalByteWiseComparator();

		public string Name { get { return "CaseInsensitiveComparator"; }}

		/// <summary>
		/// Note that this probably won't do very well for case insensitive chars that uses
		/// more than a single byte. That is good enough for now.
		/// </summary>
		public int Compare(ArraySegment<byte> a, ArraySegment<byte> b)
		{
			var minLen = Math.Min(a.Count, b.Count);
			for (int i = 0; i < minLen; i++)
			{
				var cha = (char)a.Array[a.Offset + i];
				var chb = (char) b.Array[b.Offset + i];

				var diff = char.ToUpperInvariant(cha) - char.ToUpperInvariant(chb);
				if (diff != 0)
					return diff;
			}
			return a.Count - b.Count;
		}

		public int FindSharedPrefix(ArraySegment<byte> a, ArraySegment<byte> b)
		{
			return inner.FindSharedPrefix(a, b);
		}

		public ArraySegment<byte> FindShortestSeparator(ArraySegment<byte> a, ArraySegment<byte> b)
		{
			return inner.FindShortestSeparator(a, b);
		}

		public ArraySegment<byte> FindShortestSuccessor(ArraySegment<byte> key)
		{
			return inner.FindShortestSuccessor(key);
		}
	}
}