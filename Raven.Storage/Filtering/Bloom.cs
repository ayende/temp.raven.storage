using System;
using System.Collections.Generic;
using Raven.Storage.Data;

namespace Raven.Storage.Filtering
{
	public static class Bloom
	{
		public static uint Hash(IEnumerable<byte> source, int count, uint seed = 0xbc9f1d34)
		{
			// Similar to murmur hash
			const uint m = 0xc6a4a793;
			const int r = 24;
			var h = (uint)(seed ^ (count * m));
			var buffer = new byte[4];
			int size = 0;
			using (var enumerator = source.GetEnumerator())
			{
				while (enumerator.MoveNext())
				{
					buffer[size++] = enumerator.Current;
					if (size == 4)// Pick up four bytes at a time
					{
						var w = BitConverter.ToUInt32(buffer, 0);
						h += w;
						h *= m;
						h ^= (h >> 16);
					}
				}
			}
			

			// Pick up remaining bytes
			switch (size)
			{
				case 3:
					h += (uint)(buffer[2] << 16);
					goto case 2;
				case 2:
					h += (uint)(buffer[1] << 8);
					goto case 2;
				case 1:
					h += buffer[0];
					h *= m;
					h ^= (h >> r);
					break;
			}
			return h;
		}
	}
}