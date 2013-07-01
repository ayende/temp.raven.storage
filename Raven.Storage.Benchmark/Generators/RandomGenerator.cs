namespace Raven.Storage.Benchmark.Generators
{
	using System;
	using System.IO;

	public class RandomGenerator
	{
		private readonly Random random;
		private readonly byte[] _buffer;

		public RandomGenerator()
		{
			random = new Random(301);

			_buffer = new byte[1048576];
			random.NextBytes(_buffer);
		}

		public Stream Generate(int size)
		{
			int next = random.Next(0, _buffer.Length);

			var stream = new MemoryStream();
			using (var repeatingStream = new RepeatingStream(_buffer, next, size))
			{
				repeatingStream.CopyTo(stream, size);
			}

			stream.Position = 0;
			return stream;
		}

		public class RepeatingStream : Stream
		{
			private readonly byte[] _buffer;
			private int _offset;
			private readonly long _length;

			public RepeatingStream(byte[] buffer, int offset, long length)
			{
				_buffer = buffer;
				_offset = offset;
				_length = length;
			}

			public override void Flush()
			{
				throw new NotImplementedException();
			}

			public override long Seek(long offset, SeekOrigin origin)
			{
				throw new NotImplementedException();
			}

			public override void SetLength(long value)
			{
				throw new NotImplementedException();
			}

			public override int Read(byte[] buffer, int offset, int count)
			{
				if (Position >= Length)
					return 0;

				int countToRead = Math.Min((int)Length, Math.Min(count, _buffer.Length - _offset));

				Buffer.BlockCopy(_buffer, _offset, buffer, offset, countToRead);

				_offset += countToRead;
				if (_offset >= _buffer.Length)
					_offset = 0;
				Position += countToRead;

				return countToRead;
			}

			public override void Write(byte[] buffer, int offset, int count)
			{
				throw new NotImplementedException();
			}

			public override bool CanRead { get { return true; } }

			public override bool CanSeek
			{
				get { throw new NotImplementedException(); }
			}

			public override bool CanWrite
			{
				get { throw new NotImplementedException(); }
			}

			public override long Length { get { return _length; } }

			public override long Position { get; set; }
		}
	}
}