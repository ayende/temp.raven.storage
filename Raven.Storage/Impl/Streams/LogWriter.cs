using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Raven.Storage.Util;

namespace Raven.Storage.Impl.Streams
{
	public class LogWriter : IDisposable
	{
		public static readonly Dictionary<LogRecordType, uint> RecordTypeCrcs =
			((LogRecordType[])Enum.GetValues(typeof(LogRecordType)))
				.ToDictionary(x => x, x => Crc.Extend(0, (byte)x));


		private readonly Stream stream;
		private readonly BufferPool _bufferPool;
		private readonly BinaryWriter _binaryWriter;

		public const int BlockSize = 32 * 1024;
		// Header is checksum (4 bytes), length (2 bytes) + type (1 byte)
		public const int HeaderSize = 4 + 2 + 1;


		private int _bufferPos;
		private readonly byte[] _buffer;

		private LogRecordType currentRecordType = LogRecordType.ZeroType;

		private bool _flushToDisk;

		public LogWriter(Stream stream, BufferPool bufferPool)
		{
			this.stream = stream;
			_bufferPool = bufferPool;
			_buffer = bufferPool.Take(BlockSize);
			_binaryWriter = new BinaryWriter(stream);
			_bufferPos = HeaderSize;
			_flushToDisk = true;
		}

		public void RecordStarted(bool flushToDisk = true)
		{
			_flushToDisk = flushToDisk;
			currentRecordType = LogRecordType.FullType;
		}

		public async Task RecordCompletedAsync()
		{
			await FlushBuffer(recordCompleted: true);
			currentRecordType = LogRecordType.ZeroType;
			_flushToDisk = true;
		}

		public async Task WriteAsync(byte[] buffer, int offset, int count)
		{
			if (currentRecordType == LogRecordType.ZeroType)
			{
				throw new InvalidOperationException("Did you forget to call RecordStarted() ? ");
			}
			do
			{
				var leftover = _buffer.Length - _bufferPos;
				if (leftover < HeaderSize)
				{
					// not enough space for a record, fill with nulls & flush
					for (int i = 0; i < leftover; i++)
					{
						_buffer[_bufferPos++] = 0;
					}
					await FlushBuffer(recordCompleted: false);
					_bufferPos = HeaderSize;
				}
				var avail = BlockSize - _bufferPos;// - HeaderSize, _bufferPos already starts there
				// Invariant: we never leave < HeaderSize bytes in a block.
				Debug.Assert(avail >= 0);

				var len = Math.Min(count, avail);
				Buffer.BlockCopy(buffer, offset, _buffer, _bufferPos, len);
				_bufferPos += len;
				offset += len;
				count -= len;
			} while (count > 0);
		}

		private async Task FlushBuffer(bool recordCompleted)
		{
			switch (currentRecordType)
			{
				case LogRecordType.FullType:
					currentRecordType = recordCompleted ? LogRecordType.FullType : LogRecordType.StartType;
					break;
				case LogRecordType.StartType:
					currentRecordType = recordCompleted ? LogRecordType.EndType : LogRecordType.MiddleType;
					break;
				case LogRecordType.MiddleType:
					currentRecordType = recordCompleted ? LogRecordType.EndType : LogRecordType.MiddleType;
					break;
				default:
					throw new ArgumentOutOfRangeException("Cannot flush when the currentRecordType is: " + currentRecordType);
			}

			await EmitPhysicalRecord(currentRecordType, _buffer, HeaderSize, _bufferPos - HeaderSize);

			if (recordCompleted)
			{
				currentRecordType = LogRecordType.FullType;
				_bufferPos = HeaderSize;
			}
		}

		public void Dispose()
		{
			_bufferPool.Return(_buffer);
			stream.Dispose();
		}

		private Task EmitPhysicalRecord(LogRecordType type, byte[] buffer, int offset, int count)
		{
			return Task.Run(
				() =>
				{
					// calc crc & write header
					var crc = Crc.Extend(RecordTypeCrcs[type], buffer, offset, count);
					_binaryWriter.Write(Crc.Mask(crc));
					_binaryWriter.Write((ushort)count);
					_binaryWriter.Write((byte)type);
					_binaryWriter.Write(buffer, offset, count);

					if (_flushToDisk)
						_binaryWriter.Flush();

					_bufferPos += HeaderSize;
				});
		}

		public async Task CopyFromAsync(Stream incoming)
		{
			var bytes = _bufferPool.Take(BlockSize);
			try
			{
				while (true)
				{
					var reads = incoming.Read(bytes, 0, bytes.Length);
					if (reads == 0)
						break;
					await WriteAsync(bytes, 0, reads);
				}
			}
			finally
			{
				_bufferPool.Return(bytes);
			}
		}
	}
}