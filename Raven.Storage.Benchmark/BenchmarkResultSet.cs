﻿namespace Raven.Storage.Benchmark
{
	using System.Collections.Generic;
	using System.Linq;

	internal class BenchmarkResultSet
	{
		public string Benchmark { get; private set; }

		private readonly BenchmarkParameters parameters;

		private readonly BenchmarkResult[] results;

		public BenchmarkResultSet(string benchmark, BenchmarkParameters parameters)
		{
			this.Benchmark = benchmark;
			this.parameters = parameters;

			results = new BenchmarkResult[parameters.NumberOfThreads];
		}

		public void AddResult(int index, BenchmarkResult result)
		{
			result.StopTimer();
			results[index] = result;
		}

		public double ElapsedSeconds
		{
			get
			{
				return results.Max(x => x.Stopwatch.Elapsed.TotalSeconds);
			}
		}

		public double ElapsedMilliseconds
		{
			get
			{
				return results.Max(x => x.Stopwatch.Elapsed.TotalMilliseconds);
			}
		}

		public string Rate
		{
			get
			{
				return string.Format("{0:1} MB/s", (TotalBytes / 1048576.0) / ElapsedSeconds);
			}
		}

		public long TotalBytes
		{
			get
			{
				return results.Sum(x => x.Bytes);
			}
		}

		public long TotalOperations
		{
			get
			{
				return results.Sum(x => x.Operations);
			}
		}

		public IList<string> Messages
		{
			get
			{
				return this.results.SelectMany(result => result.Messages).ToList();
			}
		}
	}
}