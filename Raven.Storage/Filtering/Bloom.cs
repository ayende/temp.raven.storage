using System;
using Raven.Storage.Data;

namespace Raven.Storage.Filtering
{
	public static class Bloom
	{
		// Similar to murmur hash
		const uint m = 0xc6a4a793;
		const int r = 24;
			
		public static uint Hash(Slice key, uint seed = 0xbc9f1d34)
		{
			var h = seed ^ ((uint)key.Count * m);
			int current = 0;
			for (; current + 4 <= key.Count; current += 4)
			{
				var w = BitConverter.ToUInt32(key.Array, key.Offset + current);// Pick up four bytes at a time
				h += w;
				h *= m;
				h ^= (h >> 16);
			}

			// Pick up remaining bytes
			switch (key.Count - current)
			{
				case 3:
					h += (uint)(key.Array[key.Offset + current+2] << 16);
					goto case 2;
				case 2:
					h += (uint)(key.Array[key.Offset + current + 1] << 8);
					goto case 1;
				case 1:
					h += key.Array[key.Offset + current];
					h *= m;
					h ^= (h >> r);
					break;
			}
			return h;
		}

		private static byte ToUpper(byte b)
		{
			return (byte) char.ToUpperInvariant((char) b);
		}

		/// <summary>
		/// Intentional copying of the methods, this is a high perf portion of the codebase,
		/// and it is worth the code duplication to avoid virtual / delegate indirection
		/// </summary>
		public static uint HashCaseInsensitive(Slice key, uint seed = 0xbc9f1d34)
		{
			var h = (uint)(seed ^ (key.Count * m));
			int current = 0;
			for (; current + 4 <= key.Count; current += 4)
			{
				// Pick up four bytes at a time
				var w = (uint) (
					        ToUpper(key.Array[key.Offset + current]) << 24 |
					        ToUpper(key.Array[key.Offset + current + 1]) << 16 |
					        ToUpper(key.Array[key.Offset + current + 2]) << 8 |
					        ToUpper(key.Array[key.Offset + current + 3])
				        );
				h += w;
				h *= m;
				h ^= (h >> 16);
			}

			// Pick up remaining bytes
			switch (key.Count - current)
			{
				case 3:
					h += (uint)(ToUpper(key.Array[key.Offset + current + 2]) << 16);
					goto case 2;
				case 2:
					h += (uint)(ToUpper(key.Array[key.Offset + current + 1]) << 8);
					goto case 1;
				case 1:
					h += ToUpper(key.Array[key.Offset + current]);
					h *= m;
					h ^= (h >> r);
					break;
			}
			return h;
		}
	}
}