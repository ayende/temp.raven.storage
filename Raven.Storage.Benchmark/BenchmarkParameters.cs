namespace Raven.Storage.Benchmark
{
	using System;
	using System.Threading.Tasks;

	internal class BenchmarkParameters
	{
		public BenchmarkParameters(BenchmarkOptions options)
		{
			Num = options.Num;
			Reads = options.Reads < 0 ? options.Num : options.Reads;
			ValueSize = options.ValueSize;
			EntriesPerBatch = 1;
			FreshDatabase = false;
			Sync = false;
			NumberOfThreads = options.Threads;
			Histogram = options.Histogram;
		}

		public int Num { get; set; }

		public int Reads { get; set; }

		public int ValueSize { get; set; }

		public int EntriesPerBatch { get; set; }

		public object WriteOptions { get; set; }

		public int NumberOfThreads { get; set; }

		public bool FreshDatabase { get; set; }

		public Func<BenchmarkParameters, Task<BenchmarkResult>> Method { get; set; }

		public bool Histogram { get; set; }

		public bool Sync { get; set; }
	}
}