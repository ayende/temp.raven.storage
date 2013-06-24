namespace Raven.Storage.Util
{
	using System;
	using System.IO;

	public static class StreamExtensions
	{
		/// <summary>
		/// Read the requested number of bytes from the stream into the buffer
		/// </summary>
		/// <param name="stream">Stream to read from</param>
		/// <param name="buffer">Buffer to read into</param>
		/// <param name="toRead">Requested number of bytes</param>
		public static void ReadExactly(this Stream stream, byte[] buffer, int toRead)
		{
			var remainingToRead = toRead;

			int currentRead;
			do
			{
				currentRead = stream.Read(buffer, 0, remainingToRead);
				remainingToRead -= currentRead;

			} while (remainingToRead != 0 && currentRead != 0);

			if (remainingToRead != 0)
				throw new InvalidDataException(
					string.Format("They have read less bytes than requested. They read {0} bytes while the requested number of bytes was {1}.",
					              (toRead - remainingToRead), toRead));
		}

		public static bool AreEqual(this Stream stream, Stream other)
		{
			if (other == null)
				return false;

			var position = stream.Position;
			var otherPosition = other.Position;

			var areEqual = AreEqualInternal(stream, other);

			stream.Position = position;
			other.Position = otherPosition;

			return areEqual;
		}

		private static bool AreEqualInternal(Stream stream, Stream other)
		{
			if (other == null)
				return false;

			const int BufferSize = 2048 * 2;
			var buffer1 = new byte[BufferSize];
			var buffer2 = new byte[BufferSize];

			while (true)
			{
				int count1 = stream.Read(buffer1, 0, BufferSize);
				int count2 = other.Read(buffer2, 0, BufferSize);

				if (count1 != count2)
				{
					return false;
				}

				if (count1 == 0)
				{
					return true;
				}

				var iterations = (int)Math.Ceiling((double)count1 / sizeof(Int64));
				for (int i = 0; i < iterations; i++)
				{
					if (BitConverter.ToInt64(buffer1, i * sizeof(Int64)) != BitConverter.ToInt64(buffer2, i * sizeof(Int64)))
					{
						return false;
					}
				}
			}
		}

		public static void CopyTo(this Stream stream, Stream dest, long bytesToCopy, int bufferSize = 4096)
		{
			var buffer = new byte[32768];
			int read;
			while (bytesToCopy > 0 && (read = stream.Read(buffer, 0, (int) Math.Min(buffer.Length, bytesToCopy))) > 0)
			{
				dest.Write(buffer, 0, read);
				bytesToCopy -= read;
			}

			dest.Position = 0;
		}
	}
}