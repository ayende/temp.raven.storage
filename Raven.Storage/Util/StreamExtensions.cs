namespace Raven.Storage.Util
{
	using System;
	using System.IO;

	public static class StreamExtensions
	{
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
	}
}