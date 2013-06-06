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
	}
}