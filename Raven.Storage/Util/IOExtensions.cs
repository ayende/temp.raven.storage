﻿using System;
using System.IO;
using System.Threading.Tasks;
using Raven.Storage.Impl.Streams;
using Raven.Storage.Memory;

namespace Raven.Storage.Util
{
	using Raven.Storage.Data;

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
			byte[] buffer;
			int size;
			Get7BitsBuffer(value, out buffer, out size);
			stream.Write(buffer, 0, size);
			return size;
		}

		public static int WriteLengthPrefixedSlice(this Stream stream, Slice slice)
		{
			var size = stream.Write7BitEncodedInt(slice.Count);
			stream.Write(slice.Array, slice.Offset, slice.Count);

			return size + slice.Count;
		}

		public static Task<int> WriteLengthPrefixedSliceAsync(this LogWriter stream, Slice slice)
		{
			return stream.Write7BitEncodedIntAsync(slice.Count)
			             .ContinueWith(writeBitTask =>
				             {
					             writeBitTask.AssertNotFaulted();

					             return stream.WriteAsync(slice.Array, slice.Offset, slice.Count)
					                          .ContinueWith(writeSliceTask =>
						                          {
							                          writeSliceTask.AssertNotFaulted();

							                          return writeBitTask.Result + slice.Count;
						                          });
				             }).Unwrap();
		}

		public static int WriteLengthPrefixedSlice(this LogWriter stream, Slice slice)
		{
			return stream.Write7BitEncodedInt(slice.Count) + stream.Write(slice.Array, slice.Offset, slice.Count);
		}

		public static int WriteLengthPrefixedInternalKey(this Stream stream, InternalKey internalKey)
		{
			return WriteLengthPrefixedSlice(stream, internalKey.TheInternalKey);
		}

		public static Task<int> WriteLengthPrefixedInternalKeyAsync(this LogWriter stream, InternalKey internalKey)
		{
			return WriteLengthPrefixedSliceAsync(stream, internalKey.TheInternalKey);
		}

		public static int WriteLengthPrefixedInternalKey(this LogWriter stream, InternalKey internalKey)
		{
			return WriteLengthPrefixedSlice(stream, internalKey.TheInternalKey);
		}

		public static Slice ReadLengthPrefixedSlice(this Stream stream)
		{
			var size = stream.Read7BitEncodedInt();

			var buffer = new byte[size];
			stream.ReadExactly(buffer, size);

			return new Slice(buffer);
		}

		public static InternalKey ReadLengthPrefixedInternalKey(this Stream stream)
		{
			return new InternalKey(ReadLengthPrefixedSlice(stream));
		}

		public static Task<int> Write7BitEncodedIntAsync(this Stream stream, int value)
		{
			byte[] buffer;
			int size;
			Get7BitsBuffer(value, out buffer, out size);
			return stream.WriteAsync(buffer, 0, size).ContinueWith(t =>
				{
					t.AssertNotFaulted();

					return size;
				});
		}

		public static Task<int> Write7BitEncodedIntAsync(this LogWriter stream, int value)
		{
			byte[] buffer;
			int size;
			Get7BitsBuffer(value, out buffer, out size);
			return stream.WriteAsync(buffer, 0, size);
		}

		public static int Write7BitEncodedInt(this LogWriter stream, int value)
		{
			byte[] buffer;
			int size;
			Get7BitsBuffer(value, out buffer, out size);
			return stream.Write(buffer, 0, size);
		}

		public static Task<int> Write7BitEncodedLongAsync(this LogWriter stream, long value)
		{
			byte[] buffer;
			int size;
			Get7BitsBuffer(value, out buffer, out size);
			return stream.WriteAsync(buffer, 0, size);
		}

		public static int Write7BitEncodedLong(this LogWriter stream, long value)
		{
			byte[] buffer;
			int size;
			Get7BitsBuffer(value, out buffer, out size);
			return stream.Write(buffer, 0, size);
		}

		public static int Write7BitEncodedLong(this Stream stream, long value)
		{
			byte[] buffer;
			int size;
			Get7BitsBuffer(value, out buffer, out size);
			stream.Write(buffer, 0, size);
			return size;
		}

		public static void WriteInt32(this Stream stream, int value)
		{
			var buffer = new[]
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
				array[offset + i] = (byte)(value >> i * 8);
			}
		}

		public static ulong ReadLong(this byte[] array, int offset)
		{
			unsafe
			{
				fixed (byte* pArray = array)
				{
					byte* ptr = pArray + offset;
					ulong val = 0;

					for (int i = 0; i < sizeof (ulong); i++)
					{
						val |= (ulong) *(ptr + i) << i*8;
					}
					return val;
				}
			}
		}

		private static void Get7BitsBuffer(int value, out byte[] buffer, out int size)
		{
			Get7BitsBuffer((long)value, out buffer, out size);
		}

		private static void Get7BitsBuffer(long value, out byte[] buffer, out int size)
		{
			buffer = new byte[5];
			size = 0;
			var num = (ulong)value;
			while (num >= 128U)
			{
				buffer[size++] = ((byte)(num | 128U));
				num >>= 7;
			}
			buffer[size++] = (byte)num;
		}
	}
}
