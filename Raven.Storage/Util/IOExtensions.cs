using System;
using System.IO;
using System.IO.MemoryMappedFiles;

namespace Raven.Storage.Util
{
    public static class IOExtensions
    {
		public static int Read7BitEncodedInt(this MemoryMappedViewAccessor accessor, ref int pos)
		{
			int shift = 0;
			int val = 0;
			while (shift < 35)
			{
				var b = accessor.ReadByte(pos++);
				val |= ((b & 0x7f) << shift);
				if ((b & 0x80) == 0)
					return val;
				shift += 7;
			}
			throw new FormatException("Too many bytes in for a 7 bit encoded Int32.");
		}

		public static long Read7BitEncodedLong(this MemoryMappedViewAccessor accessor, ref int pos)
		{
			int shift = 0;
			long val = 0;
			while (shift < 70)
			{
				var b = accessor.ReadByte(pos++);
				val |= ((b & 0x7f) << shift);
				if ((b & 0x80) == 0)
					return val;
				shift += 7;
			}
			throw new FormatException("Too many bytes in for a 7 bit encoded Int32.");
		}

        public static int Write7BitEncodedInt(this Stream stream, int value)
        {
            int size = 0;
            var num = (uint)value;
            while (num >= 128U)
            {
                size++;
                stream.WriteByte((byte)(num | 128U));
                num >>= 7;
            }
            stream.WriteByte((byte)num);
            return size + 1;
        }

        public static int Write7BitEncodedLong(this Stream stream, long value)
        {
            int size = 0;
            var num = (ulong)value;
            while (num >= 128U)
            {
                size++;
                stream.WriteByte((byte)(num | 128U));
                num >>= 7;
            }
            stream.WriteByte((byte)num);
            return size + 1;
        }

        public static void Write32BitInt(this Stream stream, int value)
        {
            var buffer = new byte[4]
                {
                    (byte) value,
                    (byte) (value >> 8),
                    (byte) (value >> 16),
                    (byte) (value >> 24)
                };
            stream.Write(buffer, 0, 4);
        } 
    }
}