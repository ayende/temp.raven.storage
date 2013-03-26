using System;
using System.Collections.Generic;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Linq;
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
		private readonly bool _caseInsensitive;
		private readonly int _k;

		public BloomFilterPolicy(int bitsPerKey = 10, bool caseInsensitive = true)
		{
			_bitsPerKey = bitsPerKey;
			_caseInsensitive = caseInsensitive;
			// We intentionally round down to reduce probing cost a little bit
			_k = (int)(_bitsPerKey * 0.69); //  0.69 =~ ln(2)
			_k = Math.Max(1, Math.Min(30, _k));
		}

		public IFilterBuilder CreateBuilder()
		{
			return new BloomFilterBuilder(_bitsPerKey, _k, this);
		}

		public IFilter CreateFilter(MemoryMappedViewAccessor accessor)
		{
			if (accessor.Capacity < 5) // 1 byte for base, 4 for start of offset array
				return null;

			var baseLg = accessor.ReadByte(accessor.Capacity - 1);
			var lastWord = accessor.ReadInt32(accessor.Capacity - 5);
			if (lastWord > accessor.Capacity - 5)
				return null;

			return new BloomFilter(baseLg, lastWord, accessor, this);
		}

		public string Name { get { return "BuiltinBloomFilter"; } }


		public uint HashKey(Slice key)
		{
			var enumerable = key.Array.Skip(key.Offset).Take(key.Count);
			if (_caseInsensitive)
			{
				enumerable = enumerable.Select(b => (byte) char.ToUpperInvariant((char) b));
			}
			return Bloom.Hash(enumerable, key.Count);
		}
	}
}