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


		private readonly FileSystem _fileSystem;
		private readonly BufferPool _bufferPool;
		private readonly BinaryWriter _binaryWriter;

		public const int BlockSize = 32 * 1024;
		// Header is checksum (4 bytes), length (2 bytes) + type (1 byte)
		public const int HeaderSize = 4 + 2 + 1;

		private int _bufferPos;
		private readonly byte[] _buffer;

		private LogRecordType currentRecordType = LogRecordType.ZeroType;

		private bool _isFileSteam;

		private long _lastCompletedRecordStreamLength;

		public LogWriter(FileSystem fileSystem,Stream stream, BufferPool bufferPool)
		{
			_isFileSteam = stream is FileStream;
			_fileSystem = fileSystem;
			_bufferPool = bufferPool;
			_buffer = bufferPool.Take(BlockSize);
			_binaryWriter = new BinaryWriter(stream);
			_bufferPos = HeaderSize;

			_lastCompletedRecordStreamLength = 0;
		}

		public void RecordStarted()
		{
			currentRecordType = LogRecordType.FullType;
		}

		public void RecordCompleted(bool flushToDisk)
		{
			FlushBuffer(recordCompleted: true, force: true, flushToDisk: flushToDisk);
			_lastCompletedRecordStreamLength = _binaryWriter.BaseStream.Length;
			currentRecordType = LogRecordType.ZeroType;
		}

		public void ResetToLastCompletedRecord()
		{
			_binaryWriter.BaseStream.SetLength(_lastCompletedRecordStreamLength);
			_fileSystem.Flush(_binaryWriter.BaseStream,true);
		}

		public Task<int> WriteAsync(byte[] buffer, int offset, int count)
		{
			return Task.Run(() => Write(buffer, offset, count));
		}

		public int Write(byte[] buffer, int offset, int count)
		{
			if (currentRecordType == LogRecordType.ZeroType)
				throw new InvalidOperationException("Did you forget to call RecordStarted() ? ");

			var writtenBytes = count;

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
					FlushBuffer(recordCompleted: false, force: false, flushToDisk: false);
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

			return writtenBytes;
		}

		private void FlushBuffer(bool recordCompleted, bool force, bool flushToDisk)
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

			EmitPhysicalRecord(currentRecordType, _buffer, HeaderSize, _bufferPos - HeaderSize, force, flushToDisk);

			if (recordCompleted)
			{
				currentRecordType = LogRecordType.FullType;
				_bufferPos = HeaderSize;
			}
		}

		public void Dispose()
		{
			_bufferPool.Return(_buffer);
			_binaryWriter.Dispose();
		}

		private void EmitPhysicalRecord(LogRecordType type, byte[] buffer, int offset, int count, bool force, bool flushToDisk)
		{
			// calc crc & write header
			var crc = Crc.Extend(RecordTypeCrcs[type], buffer, offset, count);
			_binaryWriter.Write(Crc.Mask(crc));
			_binaryWriter.Write((ushort)count);
			_binaryWriter.Write((byte)type);
			_binaryWriter.Write(buffer, offset, count);

			if (force)
				_fileSystem.Flush(_binaryWriter.BaseStream, flushToDisk);

			_bufferPos += HeaderSize;
		}

		public void CopyFrom(Stream incoming)
		{
			var bytes = _bufferPool.Take(BlockSize);
			try
			{
				while (true)
				{
					var reads = incoming.Read(bytes, 0, bytes.Length);
					if (reads == 0)
						break;
					Write(bytes, 0, reads);
				}
			}
			finally
			{
				_bufferPool.Return(bytes);
			}
		}
	}
}