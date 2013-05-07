using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using Raven.Storage.Util;
using System.Linq;

namespace Raven.Storage.Impl.Streams
{
	public class LogWriterStream : Stream
	{
		private static readonly Dictionary<LogRecordType, uint> recordTypeCrcs =
			((LogRecordType[]) Enum.GetValues(typeof (LogRecordType)))
				.ToDictionary(x => x, x => Crc.CalculateCrc(0, (byte) x));
			

		private readonly Stream stream;
		private readonly BinaryWriter binaryWriter;

		public const int BlockSize = 32 * 1024;
		// Header is checksum (4 bytes), type (1 byte), length (2 bytes).
		public const int HeaderSize = 4 + 1 + 2;

		private int blockPos;

		public LogWriterStream(Stream stream)
		{
			this.stream = stream;
			binaryWriter = new BinaryWriter(stream);
		}

		public override void Flush()
		{
			stream.Flush();
		}

		public override long Seek(long offset, SeekOrigin origin)
		{
			throw new NotSupportedException();
		}

		public override void SetLength(long value)
		{
			throw new NotSupportedException();
		}

		public override int Read(byte[] buffer, int offset, int count)
		{
			throw new NotSupportedException();
		}

		public override void Write(byte[] buffer, int offset, int count)
		{
			var begin = true;
			do
			{
				var leftover = BlockSize - blockPos;
				if (leftover < HeaderSize)
				{
					for (int i = 0; i < leftover; i++)
					{
						stream.WriteByte(0);
					}
					blockPos = 0;
				}

				// Invariant: we never leave < HeaderSize bytes in a block.
				Debug.Assert(BlockSize - blockPos - HeaderSize >= 0);

				var avail = BlockSize - blockPos - HeaderSize;

				var fragmentLength = (count < avail) ? count : avail;

				var end = count == fragmentLength;


				LogRecordType type;
				if (begin && end)
				{
					type = LogRecordType.FullType;
				}
				else if (begin)
				{
					type = LogRecordType.FirstType;
				}
				else if (end)
				{
					type = LogRecordType.EndType;
				}
				else
				{
					type = LogRecordType.MiddleType;
				}

				EmitPhysicalRecord(type, buffer, offset, count);

				count -= fragmentLength;
				offset += fragmentLength;

				begin = false;
			} while (count > 0);
		}

		public override bool CanRead
		{
			get { return false; }
		}

		public override bool CanSeek
		{
			get { return false; }
		}

		public override bool CanWrite
		{
			get { return true; }
		}

		public override long Length
		{
			get { throw new NotSupportedException(); }
		}

		public override long Position
		{
			get { throw new NotSupportedException(); }
			set { throw new NotSupportedException(); }
		}

		private void EmitPhysicalRecord(LogRecordType type, byte[] buffer, int offset, int count)
		{
			// calc crc & write header
			var crc = Crc.CalculateCrc(recordTypeCrcs[type], buffer, offset, count);
			binaryWriter.Write(Crc.Mask(crc));
			binaryWriter.Write((short) count);
			binaryWriter.Write((byte) type);

			stream.Write(buffer, offset, count);
			stream.Flush();

			blockPos += count + HeaderSize;
		}
	}

	public enum LogRecordType : byte
	{
		ZeroType = 0, // reserved for zero allocated file
		FullType = 1,

		// fragments
		FirstType = 2,
		MiddleType = 3,
		EndType = 4
	}
}