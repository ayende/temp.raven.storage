namespace Raven.Storage.Benchmark
{
	using System.Collections.Generic;
	using System.Diagnostics;

	internal class BenchmarkResult
	{
		private readonly List<string> messages;

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

		public BenchmarkResult()
		{
			messages = new List<string>();

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