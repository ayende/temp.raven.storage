using System;
using System.Collections.Generic;
using System.IO;
using System.IO.MemoryMappedFiles;
using Raven.Storage.Data;

namespace Raven.Storage.Filtering
{
	/// <summary>
	/// Return a new filter policy that uses a bloom filter with approximately
	/// the specified number of bits per key.  A good value for bits_per_key
	/// is 10, which yields a filter with ~ 1% false positive rate.
	/// </summary>
	public class BloomFilterPolicy : IFilterPolicy
	{
		private readonly int _bitsPerKey;
		private readonly int _k;
		public BloomFilterPolicy(int bitsPerKey = 10)
		{
			_bitsPerKey = bitsPerKey;
			// We intentionally round down to reduce probing cost a little bit
			_k = (int)(_bitsPerKey * 0.69); //  0.69 =~ ln(2)
			_k = Math.Max(1, Math.Min(30, _k));
		}

		public IFilterBuilder CreateBuilder()
		{
			return new BloomFilterBuilder(_bitsPerKey, _k);
		}

		public IFilter CreateFilter(MemoryMappedViewAccessor accessor)
		{
			if (accessor.Capacity < 5) // 1 byte for base, 4 for start of offset array
				return null;

			var baseLg = accessor.ReadByte(accessor.Capacity - 1);
			var lastWord = accessor.ReadInt32(accessor.Capacity - 5);
			if (lastWord > accessor.Capacity - 5)
				return null;

			return new BloomFilter(baseLg, lastWord, accessor);
		}

		public string Name { get { return "BuiltinBloomFilter"; } }

		public void WriteFilter(List<Slice> keys, Stream output)
		{
			int bits = keys.Count * _bitsPerKey;
			bits = Math.Max(64, bits);
			int bytes = (bits + 7) / 8;
			bits = bytes * 8;


			var buffer = new byte[bytes];
			// remember number of probes in filter
			buffer[0] = (byte)_k;
			foreach (var key in keys)
			{
				// Use double-hashing to generate a sequence of hash values.
				// See analysis in [Kirsch,Mitzenmacher 2006].
				var h = Bloom.Hash(key);
				uint delta = (h >> 17) | (h << 15); // rotate right 17 bits
				for (int i = 0; i < _k; i++)
				{
					var bitpos = (int)(h % bits);
					buffer[bitpos / 8] |= (byte)(1 << (bitpos % 8));
					h += delta;
				}
			}
			output.Write(buffer, 0, bytes);
		}
	}

	public class BloomFilter : IFilter
	{
		private readonly byte _baseLg;
		private readonly MemoryMappedViewAccessor _accessor;
		private readonly int _offset;
		private readonly long _num;

		public BloomFilter(byte baseLg, int offset, MemoryMappedViewAccessor accessor)
		{
			_baseLg = baseLg;
			_accessor = accessor;

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

			uint h= Bloom.Hash(key);
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