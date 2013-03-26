using System;
using Raven.Storage.Data;

namespace Raven.Storage.Filtering
{
	public static class Bloom
	{
		public static uint Hash(Slice key, uint seed = 0xbc9f1d34)
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