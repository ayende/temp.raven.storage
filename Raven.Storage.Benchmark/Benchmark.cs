namespace Raven.Storage.Benchmark
{
	using System;
	using System.Diagnostics;
	using System.Globalization;
	using System.IO;
	using System.Threading.Tasks;

	using Raven.Storage.Benchmark.Env;
	using Raven.Storage.Benchmark.Generators;
	using Raven.Storage.Filtering;

	internal class Benchmark : IDisposable
	{
		private readonly BenchmarkOptions options;

		private readonly Action<string> output;

		private Storage storage;

		public Benchmark(BenchmarkOptions options, Action<string> output)
		{
			this.options = options;
			this.output = output;
		}

		public void Run()
		{
			PrintHeader();

			Open();

			foreach (var benchmark in options.Benchmarks)
			{
				var parameters = CreateBenchmarkParameters(benchmark);
				if (parameters.FreshDatabase)
					RefreshDatabase(parameters);

				if (parameters.Method == null)
					continue;

				var result = this.RunBenchmark(benchmark, parameters);
				Report(result);
			}
		}

		private void Report(BenchmarkResultSet result)
		{
			Output("Report for benchmark:		{0}", result.Benchmark);
			Output("Bytes:				{0}", result.TotalBytes);
			Output("Time (seconds):			{0:00}", result.ElapsedSeconds);
			Output("Rate:				{0}", result.Rate);
			Output("Operations:			{0}", result.TotalOperations);
			Output("Operations per second:		{0:0} op/s", result.TotalOperations / result.ElapsedSeconds);
			Output(Constants.Separator);
		}

		private void RefreshDatabase(BenchmarkParameters parameters)
		{
			Debug.Assert(parameters.FreshDatabase);

			if (options.UseExistingDatabase)
			{
				Output("{0} : skipped (--use-existing-db is true");
				parameters.Method = null;
				return;
			}

			Open();
		}

		private BenchmarkResultSet RunBenchmark(string benchmark, BenchmarkParameters parameters)
		{
			Debug.Assert(parameters.Method != null);

			var result = new BenchmarkResultSet(benchmark, parameters);

			Parallel.For(0, parameters.NumberOfThreads, i => result.AddResult(i, parameters.Method(parameters)));

			return result;
		}

		private BenchmarkParameters CreateBenchmarkParameters(string benchmark)
		{
			var parameters = new BenchmarkParameters(options);

			switch (benchmark.ToLower())
			{
				case "fillseq":
					parameters.FreshDatabase = true;
					parameters.Method = WriteSeq;
					break;
				case "fillbatch":
					parameters.FreshDatabase = true;
					parameters.EntriesPerBatch = 1000;
					break;
				case "fillrandom":
					parameters.FreshDatabase = true;
					parameters.Method = WriteRandom;
					break;
				case "overwrite":
					parameters.FreshDatabase = false;
					break;
				case "fillsync":
					parameters.FreshDatabase = true;
					parameters.Num /= 1000;
					//parameters.WriteOptions.sync = true;
					break;
				case "fill100k":
					parameters.FreshDatabase = true;
					parameters.Num /= 1000;
					parameters.ValueSize = 100 * 1000;
					break;
				case "readseq":
					break;
				case "readreverse":
					break;
				case "readrandom":
					break;
				case "readmissing":
					break;
				case "seekrandom":
					break;
				case "readhot":
					break;
				case "readrandomsmall":
					parameters.Reads /= 1000;
					break;
				case "deleteseq":
					break;
				case "deleterandom":
					break;
				case "readwhilewriting":
					parameters.NumberOfThreads++;
					break;
				case "compact":
					break;
				case "crc32c":
					break;
				case "acquireload":
					break;
				case "snappycomp":
					break;
				case "snappyuncomp":
					break;
				case "heapprofile":
					HeapProfile();
					break;
				case "stats":
					PrintStats("raven.storage.stats");
					break;
				case "sstables":
					PrintStats("raven.storage.sstables");
					break;
				default:
					throw new NotSupportedException("Unknown benchmark: " + benchmark);
			}

			return parameters;
		}

		private BenchmarkResult WriteRandom(BenchmarkParameters parameters)
		{
			return DoWrite(parameters, false);
		}

		private BenchmarkResult WriteSeq(BenchmarkParameters parameters)
		{
			return DoWrite(parameters, true);
		}

		private BenchmarkResult DoWrite(BenchmarkParameters parameters, bool seq)
		{
			var random = new Random();
			var generator = new RandomGenerator();

			var result = new BenchmarkResult();
			result.StartTimer();

			long bytes = 0;
			for (var i = 0; i < parameters.Num; i += parameters.EntriesPerBatch)
			{
				var batch = new WriteBatch();
				for (var j = 0; j < parameters.EntriesPerBatch; j++)
				{
					var k = seq ? i + j : random.Next() % options.Num;
					var key = string.Format("{0:0000000000000000}", k);
					batch.Put(key, generator.Generate(parameters.ValueSize));
					bytes += parameters.ValueSize + key.Length;
					result.FinishOperation();
				}

				storage.Writer.Write(batch);
			}

			result.AddBytes(bytes);

			result.StopTimer();
			return result;
		}

		private void PrintStats(string key)
		{
			throw new NotImplementedException();
		}

		private void HeapProfile()
		{
			throw new NotImplementedException();
		}

		private void Open()
		{
			if (!options.UseExistingDatabase)
				Close();

			Debug.Assert(!string.IsNullOrEmpty(options.DatabaseName));
			Debug.Assert(storage == null);

			var filterPolicy = options.BloomBits >= 0 ? new BloomFilterPolicy(options.BloomBits) : null;

			var storageOptions = new StorageOptions
									 {
										 CreateIfMissing = !options.UseExistingDatabase,
										 WriteBatchSize = options.WriteBatchSize,
										 FilterPolicy = filterPolicy
									 };

			storage = new Storage(options.DatabaseName, storageOptions);
		}

		private void PrintHeader()
		{
			PrintEnvironment();
			Output(Constants.Separator);

			const int KeySize = 16;
			Output("Keys:				{0} bytes each", KeySize);
			Output("Values:				{0} bytes each ({1} bytes after compression)", options.ValueSize, options.ValueSize * options.CompressionRatio + 0.5);
			Output("Entries:			{0}", options.Num);
			Output("Raw Size:			{0:0} MB (estimated)", ((KeySize + options.ValueSize) * options.Num) / 1048576.0);
			Output("File Size:			{0:0} MB (estimated)", ((KeySize + options.ValueSize * options.CompressionRatio) * options.Num) / 1048576.0);
			Output(Constants.Separator);

			PrintWarnings();
			Output(Constants.Separator);
		}

		private void PrintWarnings()
		{
#if DEBUG
			Output("WARNING: Debug is enabled; benchmarks unnecessarily slow");
#endif

			Output("WARNING: Snappy compression is not enabled");
		}

		private void PrintEnvironment()
		{
			Output(Environment.NewLine);

			var version = typeof(Benchmark).Assembly.GetName().Version;
			Output("Raven Storage:			{0}", version);
			Output(Constants.Separator);

			Output("CPU Cores:			{0}", CPUInfo.GetNumberOfProcessors());
			Output("CPU Cache:			{0}", CPUInfo.GetCacheSize(CacheLevel.Level1) + CPUInfo.GetCacheSize(CacheLevel.Level2) + CPUInfo.GetCacheSize(CacheLevel.Level3));
		}

		private void Output(string format, params object[] args)
		{
			if (output != null)
			{
				output(string.Format(format, args));
			}
		}

		public void Dispose()
		{
			Close();
		}

		private void Close()
		{
			if (storage == null)
				return;

			storage.Dispose();
			ClearDatabaseDirectory(storage.Name);
			storage = null;
		}

		private static void ClearDatabaseDirectory(string directory)
		{
			bool isRetry = false;

			while (true)
			{
				try
				{
					Directory.Delete(directory, true);
					break;
				}
				catch (IOException)
				{
					if (isRetry)
						throw;

					GC.Collect();
					GC.WaitForPendingFinalizers();
					isRetry = true;
				}
			}
		}
	}
}