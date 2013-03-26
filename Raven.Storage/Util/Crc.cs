namespace Raven.Storage.Util
{
	public class Crc
	{
		static private readonly uint[] table = GenerateTable();

		public const uint MaskDelta = 0xa282ead8;

		public static int Mask(uint crc)
		{
			return (int)(((crc >> 15) | (crc << 17)) + MaskDelta);
		}

		public static uint Unmask(int crc)
		{
			var rot = (uint)crc - MaskDelta;
			return ((rot >> 17) | (rot << 15));
		}

		public static uint CalculateCrc(uint crc, byte[] buffer, int offset, int count)
		{
			unchecked
			{
				for (int i = offset, end = offset + count; i < end; i++)
					crc = (crc >> 8) ^ table[(crc ^ buffer[i]) & 0xFF];
			}
			return crc;
		}

		public static uint CalculateCrc(uint crc, byte b)
		{
			unchecked
			{
				return (crc >> 8) ^ table[(crc ^ b) & 0xFF];
			}
		}

		static private uint[] GenerateTable()
		{
			unchecked
			{
				uint[] theTable = new uint[256];

				const uint poly = 0xEDB88320;
				for (uint i = 0; i < theTable.Length; i++)
				{
					uint crc = i;
					for (int j = 8; j > 0; j--)
					{
						if ((crc & 1) == 1)
							crc = (crc >> 1) ^ poly;
						else
							crc >>= 1;
					}
					theTable[i] = crc;
				}

				return theTable;
			}

		}
	}
}