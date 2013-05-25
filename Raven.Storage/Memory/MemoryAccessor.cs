using System;
using System.IO;

namespace Raven.Storage.Memory
{
	public class MemoryAccessor : IAccessor
	{
		private readonly byte[] _buffer;
		private readonly long _length;

		public MemoryAccessor(byte[] buffer, long length)
		{
			_buffer = buffer;
			_length = length;
		}

		public void Dispose()
		{

		}

		public IArrayAccessor CreateAccessor(long pos, long count)
		{
			return new MemoryArrayAccessor(_buffer, (int)pos, (int)Math.Min(count, _length - pos));
		}

		public class MemoryArrayAccessor : IArrayAccessor
		{
			private readonly byte[] _buffer;
			private readonly int _pos;
			private readonly int _count;

			public MemoryArrayAccessor(byte[] buffer, int pos, int count)
			{
				_buffer = buffer;
				_pos = pos;
				_count = count;
			}

			public void Dispose()
			{

			}

			public byte this[long i]
			{
				get { return _buffer[_pos + i]; }
			}

			public long Capacity { get { return _count; } }

			public int ReadInt32(long i)
			{
				return BitConverter.ToInt32(_buffer, _pos + (int)i);
			}

			public int ReadArray(long pos, byte[] buffer, int offset, int count)
			{
				var read = Math.Min(count, _count - (int)pos);
				Buffer.BlockCopy(_buffer, _pos + (int)pos, buffer, offset, read);
				return read;
			}
		}

		public Stream CreateStream(long pos, long count)
		{
			return new MemoryStream(_buffer, (int)pos, (int) Math.Min(count, _length - pos));
		}
	}
}