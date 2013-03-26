using System.IO.MemoryMappedFiles;
using Raven.Storage.Data;

namespace Raven.Storage.Filtering
{
	public class BloomFilter : IFilter
	{
		private readonly byte _baseLg;
		private readonly MemoryMappedViewAccessor _accessor;
		private readonly BloomFilterPolicy _bloomFilterPolicy;
		private readonly int _offset;
		private readonly long _num;

		public BloomFilter(byte baseLg, int offset, MemoryMappedViewAccessor accessor, BloomFilterPolicy bloomFilterPolicy)
		{
			_baseLg = baseLg;
			_accessor = accessor;
			_bloomFilterPolicy = bloomFilterPolicy;

			_offset = offset;
			_num = (accessor.Capacity - 5 - offset)/sizeof (int);
		}

		public void Dispose()
		{
			_accessor.Dispose();
		}

		public bool KeyMayMatch(long position, Slice key)
		{
			int index = (int) (position >> _baseLg);
			if (index >= _num)
				return true; // errors are treated as potential matches

			var start = _accessor.ReadInt32(_offset + index*sizeof (int));
			var limit = _accessor.ReadInt32(_offset + index * sizeof(int) + sizeof(int));

			if (start > limit || limit >= _accessor.Capacity - _offset)
				return false; // empty filters do no match any keys

			return KeyMayMatch(key, start, limit);
		}

		private bool KeyMayMatch(Slice key, int filterStart, int filterLimit)
		{
			int len = filterLimit - filterStart;
			if (len < 2)
				return false;

			int bits = (len - 1)*8;
			var k = _accessor.ReadByte(filterLimit - 1);
			if (k > 30)
			{
				// Reserved for potentially new encodings for short bloom filters.
				// Consider it a match.
				return true;
			}

			uint h = _bloomFilterPolicy.HashKey(key);
			uint delta = ((h >> 17) | (h << 16)); // rotate right 17 bits
			for (var i = 0; i < k; i++)
			{
				var bitpos = (int) (h%bits);
				var b = _accessor.ReadByte(filterStart + bitpos/8);
				if ((b & (1 << (bitpos%8))) == 0)
					return false;
				h += delta;
			}
			return true;
		}
	}
}