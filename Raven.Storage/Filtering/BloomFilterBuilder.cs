using System;
using System.Collections.Generic;
using System.IO;
using Raven.Storage.Data;

namespace Raven.Storage.Filtering
{
	public class BloomFilterBuilder : IFilterBuilder
	{
		private readonly int _bitsPerKey;
		private readonly int _k;
		private byte[] _buffer;

		public BloomFilterBuilder(int bitsPerKey, int k)
		{
			_bitsPerKey = bitsPerKey;
			_k = k;
		}

		public void CreateFilter(List<Slice> keys, Stream output)
		{
			int bits = keys.Count * _bitsPerKey;
			bits = Math.Max(64, bits);
			int bytes = (bits + 7) / 8;
			bits = bytes * 8;

			if (_buffer == null || _buffer.Length < bytes)
				_buffer = new byte[bytes];
			foreach (var key in keys)
			{
				// Use double-hashing to generate a sequence of hash values.
				// See analysis in [Kirsch,Mitzenmacher 2006].
				var h = Bloom.Hash(key);
				uint delta = (h >> 17) | (h << 15); // rotate right 17 bits
				for (int i = 0; i < _k; i++)
				{
					var bitpos = (int)(h % bits);
					_buffer[bitpos / 8] |= (byte)(1 << (bitpos % 8));
					h += delta;
				}
			}
			output.Write(_buffer, 0, bytes);
			// remember number of probes in filter
			output.WriteByte((byte)_k);
		}
	}
}