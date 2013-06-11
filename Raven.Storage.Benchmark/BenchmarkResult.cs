namespace Raven.Storage.Benchmark
{
	using System.Diagnostics;

	internal class BenchmarkResult
	{
		public Stopwatch Stopwatch { get; private set; }

		public long Bytes { get; private set; }

		public long Operations { get; private set; }

		public BenchmarkResult()
		{
			Stopwatch = new Stopwatch();
		}

		public void StartTimer()
		{
			Stopwatch.Start();
		}

		public void StopTimer()
		{
			Stopwatch.Stop();
		}

		public void FinishOperation()
		{
			Operations++;
		}

		public void AddBytes(long bytes)
		{
			Bytes += bytes;
		}
	}
}