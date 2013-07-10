namespace Raven.Storage.Benchmark
{
	using System;
	using System.Collections.Generic;
	using System.Diagnostics;
	using System.Threading;
	using System.Threading.Tasks;

	using Raven.Storage.Benchmark.Env;
	using Raven.Storage.Benchmark.Generators;
	using Raven.Storage.Data;

	using Timer = System.Timers.Timer;

	public class StressBenchmark
	{
		private readonly Storage storage;

		private readonly Statistics statistics;

		private readonly TimeSpan readDelayInSeconds;

		public StressBenchmark(Storage storage)
		{
			this.storage = storage;
			this.readDelayInSeconds = TimeSpan.FromSeconds(5);
			this.statistics = new Statistics(readDelayInSeconds);
		}

		private void Initialize()
		{
			Console.Clear();
			Console.WriteLine("Stress benchmark");
			Console.WriteLine("Start date: " + DateTime.Now);
			Console.WriteLine(Constants.Separator);
			Console.WriteLine("Number of reads:		0");
			Console.WriteLine("Reads per second:		0");
			Console.WriteLine("Megabytes read:			0");
			Console.WriteLine("Read exceptions:		0");
			Console.WriteLine(Constants.Separator);
			Console.WriteLine("Number of writes:		0");
			Console.WriteLine("Writes per second:		0");
			Console.WriteLine("Megabytes written:		0");
			Console.WriteLine("Write exceptions:		0");
			Console.WriteLine(Constants.Separator);
		}

		public Task RunAsync()
		{
			Initialize();

			var timer = new Timer(1000);
			timer.Elapsed += (sender, args) => Report();
			timer.Start();

			var tasks = new List<Task>
				            {
					            ProcessWrites(500, 0), 
								ProcessWrites(500, 0), 
								ProcessWrites(500, 0), 
								ProcessWrites(1000 * 500, 1000), 
								ProcessReads(),
								ProcessReads()
				            };

			return Task.WhenAll(tasks);
		}

		private Task ProcessReads()
		{
			return Task.Factory.StartNew(() =>
				{
					Thread.Sleep((int)readDelayInSeconds.TotalMilliseconds);

					while (true)
					{
						try
						{
							using (var iterator = storage.Reader.NewIterator(new ReadOptions()))
							{
								iterator.SeekToFirst();
								while (iterator.IsValid)
								{
									using (var stream = iterator.CreateValueStream())
									{
										statistics.NumberOfReads++;
										statistics.NumberOfReadsSinceLastReport++;

										statistics.BytesRead += stream.Length;
									}

									iterator.Next();
								}
							}
						}
						catch
						{
							statistics.NumberOfReadExceptions++;
						}
					}
				},
				TaskCreationOptions.LongRunning);
		}

		private async Task ProcessWrites(int size, int timeToWait)
		{
			var generator = new RandomGenerator();
			var random = new Random();

			while (true)
			{
				try
				{
					var batchSize = random.Next(1, 10);
					var batch = new WriteBatch();

					for (var i = 0; i < batchSize; i++)
					{
						var k = random.Next();
						var key = string.Format("{0:0000000000000000}", k);

						batch.Put(key, generator.Generate(size));

					}

					await storage.Writer.WriteAsync(batch);
					statistics.NumberOfWrites += batchSize;
					statistics.NumberOfWritesSinceLastReport += batchSize;
					statistics.BytesWritten += batchSize * (size + 16);

					await Task.Delay(timeToWait);
				}
				catch (Exception)
				{
					statistics.NumberOfWriteExceptions++;
				}
			}
		}

		private void Report()
		{
			ReportWrite(32, 3, string.Format("{0}", statistics.NumberOfReads)); // number of reads
			ReportWrite(32, 4, string.Format("{0:0}/s [Avg: {1:0}/s]", statistics.NumberOfReadsSinceLastReport, statistics.NumberOfReads / statistics.ElapsedSecondsForReads)); // reads per second
			ReportWrite(32, 5, string.Format("{0:0}", statistics.BytesRead / (double)(1024 * 1024))); // megabytes read
			ReportWrite(32, 6, string.Format("{0:0}", statistics.NumberOfReadExceptions)); // read ex

			statistics.NumberOfReadsSinceLastReport = 0;

			ReportWrite(32, 8, statistics.NumberOfWrites); // number of writes
			ReportWrite(32, 9, string.Format("{0:0}/s [Avg: {1:0}/s]", statistics.NumberOfWritesSinceLastReport, statistics.NumberOfWrites / statistics.ElapsedSecondsForWrites)); // writes per second
			ReportWrite(32, 10, string.Format("{0:0}", statistics.BytesWritten / (double)(1024 * 1024))); // megabytes written
			ReportWrite(32, 11, statistics.NumberOfWriteExceptions); // write ex

			statistics.NumberOfWritesSinceLastReport = 0;

			Console.SetCursorPosition(0, 12); // reset
		}

		private void ReportWrite(int left, int top, object text)
		{
			Console.SetCursorPosition(left, top);
			Console.Write("                              "); // clear
			Console.SetCursorPosition(left, top);
			Console.Write(text);
		}

		private class Statistics
		{
			private readonly Stopwatch watch;

			private readonly TimeSpan readDelayInSeconds;

			public Statistics(TimeSpan readDelayInSeconds)
			{
				this.readDelayInSeconds = readDelayInSeconds;
				watch = Stopwatch.StartNew();
			}

			public double ElapsedSecondsForWrites
			{
				get
				{
					return watch.Elapsed.TotalSeconds;
				}
			}

			public double ElapsedSecondsForReads
			{
				get
				{
					var result = watch.Elapsed.TotalSeconds - readDelayInSeconds.TotalSeconds;
					if (result <= 0) 
						return 1;

					return result;
				}
			}

			public long NumberOfReads { get; set; }

			public long NumberOfReadsSinceLastReport { get; set; }

			public long NumberOfReadExceptions { get; set; }

			public long NumberOfWrites { get; set; }

			public long NumberOfWritesSinceLastReport { get; set; }

			public long NumberOfWriteExceptions { get; set; }

			public long BytesRead { get; set; }

			public long BytesWritten { get; set; }
		}
	}
}