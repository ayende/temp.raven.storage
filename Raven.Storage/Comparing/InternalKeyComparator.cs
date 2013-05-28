using System;
using System.Diagnostics;
using Raven.Storage.Data;
using Raven.Storage.Util;

namespace Raven.Storage.Comparing
{
	/// <summary>
	/// An internal comparator that uses a user comparator and breaks 
	/// ties by decreasing sequnece number
	/// </summary>
	public class InternalKeyComparator : IComparator
	{
		public string Name { get { return "InternalKeyComparator"; } }

		private readonly IComparator _comparator;

		public IComparator UserComparator
		{
			get
			{
				return _comparator;
			}
		}

		public InternalKeyComparator(IComparator comparator)
		{
			_comparator = comparator;
		}

		public int Compare(Slice a, Slice b)
		{
			var r = _comparator.Compare(ExtractUserKey(a), ExtractUserKey(b));
			if (r != 0)
				return r;

			var anum = a.Array.ReadLong(a.Count - 8);
			var bnum = b.Array.ReadLong(b.Count - 8);

			if (anum > bnum)
				return -1;

			if (bnum > anum)
				return 1;

			return 0;
		}

		private static Slice ExtractUserKey(Slice a)
		{
			Debug.Assert(a.Count >= 8);
			return new Slice(a.Array, a.Offset, a.Count - 8);
		}

		public int FindSharedPrefix(Slice a, Slice b)
		{
			var aUser = ExtractUserKey(a);
			var bUser = ExtractUserKey(b);
			var r = _comparator.FindSharedPrefix(aUser, bUser);
			if (r != aUser.Count || r != aUser.Count)
				return r;
			// the user keys are the same 100%, need to see how much of the actual
			// seq is matching as well
			var minLen = Math.Min(a.Count, b.Count);
			var pos = aUser.Count;
			while (pos < minLen && a.Array[a.Offset + pos] == b.Array[b.Offset + pos])
			{
				pos++;
			}
			return pos;
		}

		public Slice FindShortestSeparator(Slice start, Slice limit, ref byte[] scratch)
		{
			var userStart = ExtractUserKey(start);
			var userLimit = ExtractUserKey(limit);
			var r = _comparator.FindShortestSeparator(userStart, userLimit, ref scratch);

			return EnsureSortOrder(r, userStart, ref scratch);
		}

		public Slice FindShortestSuccessor(Slice key, ref byte[] scratch)
		{
			var userKey = ExtractUserKey(key);

			var r = _comparator.FindShortestSuccessor(userKey, ref scratch);

			return EnsureSortOrder(r, userKey, ref scratch);
		}

		private Slice EnsureSortOrder(Slice r, Slice origin, ref byte[] scratch)
		{
			if (r.Count >= origin.Count || _comparator.Compare(origin, r) >= 0)
				return r;

			// user key has become shorter physically, but larger logically.
			// Tack on the earliest posible number to the shorted user key
			if (r.Count + 8 >= scratch.Length || r.Array != scratch)
			{
				scratch = new byte[r.Count + 8];
				Buffer.BlockCopy(r.Array, r.Offset, scratch, 0, r.Count);
			}
			scratch.WriteLong(r.Count, Format.PackSequenceAndType(Format.MaxSequenceNumber, ItemType.ValueForSeek));

			return new Slice(scratch, 0, r.Count + 8);
		}

		public bool EqualKeys(Slice a, Slice b)
		{
			var aUser = ExtractUserKey(a);
			var bUser = ExtractUserKey(b);
			var r = _comparator.Compare(aUser, bUser);
			return r == 0;
		}
	}
}