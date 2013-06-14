namespace Raven.Storage.Benchmark
{
	using System;
	using System.Collections.Generic;
	using System.Diagnostics;

	internal class BenchmarkResult
	{
		private DateTime lastOperationFinishedDate;

		private readonly List<string> messages;

		private readonly BenchmarkParameters parameters;

		public Histogram Histogram { get; private set; }

		public Stopwatch Stopwatch { get; private set; }

		public long Bytes { get; private set; }

		public long Operations { get; private set; }

		public IReadOnlyList<string> Messages
		{
			get
			{
				return messages.AsReadOnly();
			}
		}

		public BenchmarkResult(BenchmarkParameters parameters)
		{
			this.parameters = parameters;
			lastOperationFinishedDate = DateTime.Now;

			messages = new List<string>();
			Histogram = new Histogram();

			Stopwatch = new Stopwatch();
			StartTimer();
		}

		private void StartTimer()
		{
			Stopwatch.Start();
		}

		public void StopTimer()
		{
			Stopwatch.Stop();
		}

		public void FinishOperation()
		{
			if (parameters.Histogram)
			{
				var now = DateTime.Now;
				var span = now - lastOperationFinishedDate;

				Histogram.Add(span.TotalMilliseconds);

				lastOperationFinishedDate = now;
			}

			Operations++;
		}

		public void AddBytes(long bytes)
		{
			Bytes += bytes;
		}

		public void AddMessage(string message)
		{
			messages.Add(message);
		}
	}
}