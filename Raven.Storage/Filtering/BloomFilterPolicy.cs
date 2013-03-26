using System;
using System.Collections.Generic;
using System.IO;
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

		public IFilter CreateFilter()
		{
			throw new System.NotImplementedException();
		}

		public string Name { get { return "BuiltinBloomFilter"; } }

		public void CreateFilter(List<Slice> keys, Stream output)
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
				var h = BloomFilterBuilder.BloomHash(key);
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
}