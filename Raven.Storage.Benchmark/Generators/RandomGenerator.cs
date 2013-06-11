namespace Raven.Storage.Benchmark.Generators
{
	using System;
	using System.Diagnostics;
	using System.IO;

	internal class RandomGenerator
	{
		private readonly Random random;

		private readonly Stream stream;

		public RandomGenerator()
		{
			random = new Random(301);
			stream = new MemoryStream();

			var buffer = new byte[1048576];
			random.NextBytes(buffer);

			stream.Write(buffer, 0, buffer.Length);
			stream.Position = 0;
		}

		public Stream Generate(int size)
		{
			if (stream.Position + size > stream.Length)
			{
				stream.Position = 0;
				Debug.Assert(size < stream.Length);
			}

			var buffer = new byte[size];
			stream.Read(buffer, 0, size);

			return new MemoryStream(buffer);
		}
	}
}