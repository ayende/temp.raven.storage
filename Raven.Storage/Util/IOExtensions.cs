﻿using System;
using System.IO;
using System.IO.MemoryMappedFiles;
using Raven.Storage.Memory;

namespace Raven.Storage.Util
{
    public static class IOExtensions
    {
		public static int Read7BitEncodedInt(this IArrayAccessor accessor, ref int pos)
		{
			int ret = 0;
			int shift = 0;
			int len;

			for (len = 0; len < 5; ++len)
			{
				byte b = accessor[pos++];

				ret = ret | ((b & 0x7f) << shift);
				shift += 7;
				if ((b & 0x80) == 0)
					break;
			}

			if (len < 5)
				return ret;
			throw new FormatException("Too many bytes in what should have been a 7 bit encoded Int32.");
		}

		public static int Read7BitEncodedInt(this Stream stream)
		{
			int ret = 0;
			int shift = 0;
			int len;

			for (len = 0; len < 5; ++len)
			{
				int b = stream.ReadByte();
				if (b == -1)
					throw new EndOfStreamException();

				ret = ret | (((byte)b & 0x7f) << shift);
				shift += 7;
				if ((b & 0x80) == 0)
					break;
			}

			if (len < 5)
				return ret;
			throw new FormatException("Too many bytes in what should have been a 7 bit encoded Int32.");
		}

		public static long Read7BitEncodedLong(this IArrayAccessor accessor, ref int pos)
		{
			long ret = 0;
			int shift = 0;
			int len;

			for (len = 0; len < 9; ++len)
			{
				byte b = accessor[pos++];

				ret = ret | ((b & 0x7fU) << shift);
				shift += 7;
				if ((b & 0x80) == 0)
					break;
			}

			if (len < 9)
				return ret;
			throw new FormatException("Too many bytes in what should have been a 7 bit encoded Int64.");
	
		}

		public static long Read7BitEncodedLong(this Stream stream)
		{
			long ret = 0;
			int shift = 0;
			int len;

			for (len = 0; len < 5; ++len)
			{
				int b = stream.ReadByte();
				if (b == -1)
					throw new EndOfStreamException();

				ret = ret | (((byte)b & 0x7fU) << shift);
				shift += 7;
				if ((b & 0x80) == 0)
					break;
			}

			if (len < 5)
				return ret;
			throw new FormatException("Too many bytes in what should have been a 7 bit encoded Int64.");
	
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

        public static void WriteInt32(this Stream stream, int value)
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

		public static void WriteLong(this byte[] array, int offset, ulong value)
		{
			for (int i = 0; i < sizeof(ulong); i++)
			{
				array[offset + i] = (byte) (value >> i*8);
			}
		}

		public static ulong ReadLong(this byte[] array, int offset)
		{
			ulong val = 0;
			for (int i = 0; i < sizeof(ulong); i++)
			{
				val |= (ulong) array[offset + i] << i*8;
			}
			return val;
		} 
    }
}