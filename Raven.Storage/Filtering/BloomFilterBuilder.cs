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
			// remember number of probes in filter
			_buffer[0] = (byte)_k;
			foreach (var key in keys)
			{
				// Use double-hashing to generate a sequence of hash values.
				// See analysis in [Kirsch,Mitzenmacher 2006].
				var h = BloomHash(key);
				uint delta = (h >> 17) | (h << 15); // rotate right 17 bits
				for (int i = 0; i < _k; i++)
				{
					var bitpos = (int)(h % bits);
					_buffer[bitpos / 8] |= (byte)(1 << (bitpos % 8));
					h += delta;
				}
			}
			output.Write(_buffer, 0, bytes);
		}

		public static uint BloomHash(Slice key, uint seed = 0xbc9f1d34)
		{
			// Similar to murmur hash
			const uint m = 0xc6a4a793;
			const int r = 24;
			var h = (uint)(seed ^ (key.Count * m));

			// Pick up four bytes at a time
			int current = key.Offset;
			for (; current < key.Count; current += 4)
			{
				var w = BitConverter.ToUInt32(key.Array, key.Offset + current);
				h += w;
				h *= m;
				h ^= (h >> 16);
			}

			// Pick up remaining bytes
			switch (key.Count - current)
			{
				case 3:
					h += (uint)(key.Array[key.Offset + current + 2] << 16);
					goto case 2;
				case 2:
					h += (uint)(key.Array[key.Offset + current + 2] << 8);
					goto case 2;
				case 1:
					h += key.Array[key.Offset + current + 2];
					h *= m;
					h ^= (h >> r);
					break;
			}
			return h;
		}
	}
}