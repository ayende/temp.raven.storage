// -----------------------------------------------------------------------
//  <copyright file="LogReaderStream.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.IO;
using Raven.Storage.Data;
using Raven.Storage.Exceptions;
using Raven.Storage.Util;

namespace Raven.Storage.Impl.Streams
{
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
				type = (LogRecordType)_binaryReader.ReadByte();
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