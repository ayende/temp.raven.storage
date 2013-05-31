using System;
using System.IO;
using Raven.Storage.Exceptions;
using Raven.Storage.Util;

namespace Raven.Storage.Impl.Streams
{
	public class LogReader
	{
		private readonly bool _checksum;
		private readonly long _initialOffset;
		private readonly Stream _inner;
		private readonly BufferPool _bufferPool;

		/// <summary>
		///     If checksum is true, verify checksums if available.
		///     The reader will start reading at the first record located at physical
		///     position >= initialOffset within the file.
		/// </summary>
		public LogReader(Stream inner, bool checksum, long initialOffset, BufferPool bufferPool)
		{
			_inner = inner;
			_checksum = checksum;
			_initialOffset = initialOffset;
			_bufferPool = bufferPool;

			if (inner.Position >= _initialOffset) 
				return;
			if (SkipToInitialBlock() == false)
				throw new CorruptedDataException("Cannot get to initial offset");
		}

		/// <summary>
		///     Returns the physical offset of the last record returned by ReadRecord.
		///     Undefined before the first call to ReadRecord.
		/// </summary>
		public long LastRecordOffset { get; private set; }

		/// <summary>
		///     Read the record into the stream.  Returns true if successful, false if we hit end of input.
		///     Caller must call dispose on the stream after usage.
		/// </summary>
		public bool TryReadRecord(out Stream stream)
		{
			try
			{
				stream = new LogRecordStream(_inner, _checksum, _bufferPool);
			}
			catch (EndOfStreamException)
			{
				stream = null;
				return false;
			}

			return true;
		}

		private bool SkipToInitialBlock()
		{
			long offsetInBlock = _initialOffset % LogWriter.BlockSize;
			long blockStartLocation = _initialOffset - offsetInBlock;

			// Don't search a block if we'd be in the trailer
			if (offsetInBlock > LogWriter.BlockSize - 6)
			{
				blockStartLocation += LogWriter.BlockSize;
			}

			// Skip to start of first block that can contain the initial record
			if (blockStartLocation <= 0)
				return true;

			long result = _inner.Seek(blockStartLocation, SeekOrigin.Begin);
			return result == blockStartLocation;
		}

		public class LogRecordStream : Stream
		{
			private readonly Stream _inner;
			private readonly bool _checksum;
			private readonly BufferPool _bufferPool;
			private readonly BinaryReader _binaryReader;
			private readonly byte[] _buffer;
			private int _posInBuffer;
			private int _bufferLen;
			private bool _completedRecord;
			private bool _inFragmentedRecord;

			public LogRecordStream(Stream inner, bool checksum, BufferPool bufferPool)
			{
				_inner = inner;
				_checksum = checksum;
				_bufferPool = bufferPool;
				_binaryReader = new BinaryReader(inner);
				_buffer = _bufferPool.Take(LogWriter.BlockSize);

				try
				{
					var type = ReadPhysicalRecord();
					switch (type)
					{
						case LogRecordType.FullType:
							_completedRecord = true;
							break;
						case LogRecordType.StartType:
							_inFragmentedRecord = true;
							break;
						case LogRecordType.MiddleType:
						case LogRecordType.EndType:
							throw new CorruptedDataException("missing start of fragmented record(3)");
						default:
							throw new ArgumentOutOfRangeException("Don't know how to handle record type: " + type);
					}
				}
				catch (Exception)
				{
					_bufferPool.Return(_buffer);
					throw;
				}
			}

			private LogRecordType ReadPhysicalRecord()
			{
				uint crc = Crc.Unmask(_binaryReader.ReadInt32()); // if this throws, we are at eof at header start, which is good
				ushort len;
				LogRecordType type;
				try
				{
					len = _binaryReader.ReadUInt16();
					type = (LogRecordType) _binaryReader.ReadByte();
				}
				catch (EndOfStreamException e)
				{
					throw new CorruptedDataException("could not read record header, early eof", e);
				}
				if (LogWriter.HeaderSize + len > LogWriter.BlockSize)
				{
					throw new CorruptedDataException("bad record length in header");
				}

				var pos = 0;
				int read = -1;
				while (read != 0)
				{
					read = _inner.Read(_buffer, pos, len - pos);
					pos += read;
				}

				if (pos != len)
				{
					throw new CorruptedDataException("could not read full record length, early eof");
				}

				_bufferLen = pos;
				_posInBuffer = 0;
				if (_checksum)
				{
					var actualCrc = Crc.CalculateCrc(LogWriter.RecordTypeCrcs[type], _buffer, 0, _bufferLen);
					if (actualCrc != crc)
					{
						throw new CorruptedDataException("checksum mismatch");
					}
				}

				return type;
			}

			protected override void Dispose(bool disposing)
			{
				_bufferPool.Return(_buffer);
				base.Dispose(disposing);
			}

			public override int Read(byte[] buffer, int offset, int count)
			{
				if (_posInBuffer >= _bufferLen && _completedRecord)
				{
					return 0;
				}

				if (_posInBuffer < _bufferLen)
				{
					return ReadFromBuffer(buffer, offset, count);
				}
				LogRecordType type;
				try
				{
					type = ReadPhysicalRecord();
				}
				catch (EndOfStreamException)
				{
					if (_inFragmentedRecord)
					{
						throw new CorruptedDataException("partial record without end(3)");
					}
					return 0;
				}

				switch (type)
				{
					case LogRecordType.FullType:
						if (_inFragmentedRecord)
						{
							throw new CorruptedDataException("partial record without end(1)");
						}
						_completedRecord = true;
						return ReadFromBuffer(buffer, offset, count);
					case LogRecordType.StartType:
						if (_inFragmentedRecord)
						{
							throw new CorruptedDataException("partial record without end(2)");
						}
						_inFragmentedRecord = true;
						return ReadFromBuffer(buffer, offset, count);
					case LogRecordType.MiddleType:
						if (_inFragmentedRecord == false)
						{
							throw new CorruptedDataException("missing start of fragmented record(1)");
						}
						return ReadFromBuffer(buffer, offset, count);
					case LogRecordType.EndType:
						if (_inFragmentedRecord == false)
						{
							throw new CorruptedDataException("missing start of fragmented record(2)");
						}
						_inFragmentedRecord = false;
						_completedRecord = true;
						return ReadFromBuffer(buffer, offset, count);
					default:
						throw new ArgumentOutOfRangeException("Don't know how to handle record type: " + type);
				}
			}

			private int ReadFromBuffer(byte[] buffer, int offset, int count)
			{
				count = Math.Min(count, _bufferLen - _posInBuffer);
				var read = Math.Min(count, _bufferLen - _posInBuffer);
				Buffer.BlockCopy(_buffer, _posInBuffer, buffer, offset, read);
				_posInBuffer += read;
				return read;
			}

			public override void Flush()
			{

			}

			public override long Seek(long offset, SeekOrigin origin)
			{
				throw new System.NotSupportedException();
			}

			public override void SetLength(long value)
			{
				throw new System.NotSupportedException();
			}

		
			public override void Write(byte[] buffer, int offset, int count)
			{
				throw new System.NotSupportedException();
			}

			public override bool CanRead
			{
				get { return true; }
			}

			public override bool CanSeek
			{
				get { return false; }
			}

			public override bool CanWrite
			{
				get { return false; }
			}

			public override long Length
			{
				get { throw new System.NotSupportedException(); }
			}

			public override long Position
			{
				get { throw new System.NotSupportedException(); }
				set { throw new System.NotSupportedException(); }
			}
		}


	}
}