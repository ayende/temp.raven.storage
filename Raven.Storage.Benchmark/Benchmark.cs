namespace Raven.Storage.Benchmark
{
	using System;
	using System.Diagnostics;
	using System.IO;
	using System.Threading.Tasks;

	using Raven.Abstractions.Util;
	using Raven.Storage.Benchmark.Env;
	using Raven.Storage.Benchmark.Generators;
	using Raven.Storage.Data;
	using Raven.Storage.Filtering;
	using Raven.Storage.Util;

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

		public async Task RunAsync()
		{
			PrintHeader();

			await OpenAsync();

			foreach (var benchmark in options.Benchmarks)
			{
				var parameters = CreateBenchmarkParameters(benchmark);
				if (parameters.FreshDatabase)
					await RefreshDatabaseAsync(parameters);

				if (parameters.Method == null)
					continue;

				var result = RunBenchmark(benchmark, parameters);
				Report(result, parameters);
			}
		}

		private void Report(BenchmarkResultSet result, BenchmarkParameters parameters)
		{
			Output("Report for benchmark:		{0}", result.Benchmark);
			Output("Bytes:				{0}", result.TotalBytes);
			Output("Time (seconds):			{0:00}", result.ElapsedSeconds);
			Output("Rate:				{0}", result.Rate);
			Output("Operations:			{0}", result.TotalOperations);
			Output("Operations per second:		{0:0} op/s", result.TotalOperations / result.ElapsedSeconds);

			if (parameters.Histogram)
			{
				Output("Milliseconds per op: ");
				Output(result.Histogram.ToString());
			}

			if (result.Messages.Count > 0)
			{
				Output("Messages:");
				for (var index = 0; index < result.Messages.Count; index++)
				{
					var message = result.Messages[index];
					Output(string.Format("{0}. {1}", index + 1, message));
				}
			}

			Output(Constants.Separator);
		}

		private async Task RefreshDatabaseAsync(BenchmarkParameters parameters)
		{
			Debug.Assert(parameters.FreshDatabase);

			if (options.UseExistingDatabase)
			{
				Output("{0} : skipped (--use-existing-db is true");
				parameters.Method = null;
				return;
			}

			await OpenAsync();
		}

		private BenchmarkResultSet RunBenchmark(string benchmark, BenchmarkParameters parameters)
		{
			Debug.Assert(parameters.Method != null);

			var result = new BenchmarkResultSet(benchmark, parameters);

			Parallel.For(0, parameters.NumberOfThreads, async i => result.AddResult(i, await parameters.Method(parameters)));

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
					parameters.Method = WriteSeq;
					break;
				case "fillrandom":
					parameters.FreshDatabase = true;
					parameters.Method = WriteRandom;
					break;
				case "overwrite":
					parameters.FreshDatabase = false;
					parameters.Method = WriteRandom;
					break;
				case "fillsync":
					parameters.FreshDatabase = true;
					parameters.Num /= 1000;
					parameters.Sync = true;
					parameters.Method = WriteRandom;
					break;
				case "fill100k":
					parameters.FreshDatabase = true;
					parameters.Num /= 1000;
					parameters.ValueSize = 100 * 1000;
					parameters.Method = WriteRandom;
					break;
				case "readseq":
					parameters.Method = ReadSequential;
					break;
				case "readreverse":
					parameters.Method = ReadReverse;
					break;
				case "readrandom":
					parameters.Method = ReadRandom;
					break;
				case "readmissing":
					parameters.Method = ReadMissing;
					break;
				case "seekrandom":
					parameters.Method = SeekRandom;
					break;
				case "readhot":
					parameters.Method = ReadHot;
					break;
				case "readrandomsmall":
					parameters.Reads /= 1000;
					parameters.Method = ReadRandom;
					break;
				case "deleteseq":
					parameters.Method = DeleteSeq;
					break;
				case "deleterandom":
					parameters.Method = DeleteRandom;
					break;
				case "readwhilewriting":
					parameters.Method = ReadWhileWriting;
					break;
				case "compact":
					parameters.Method = Compact;
					break;
				case "crc32c":
					parameters.Method = Crc32c;
					break;
				case "acquireload":
					//parameters.Method = AcquireLoad;
					break;
				case "snappycomp":
					//parameters.Method = SnappyCompress;
					break;
				case "snappyuncomp":
					//parameters.Method = SnappyUncompress;
					break;
				//case "heapprofile":
				//	HeapProfile();
				//	break;
				//case "stats":
				//	PrintStats("raven.storage.stats");
				//	break;
				//case "sstables":
				//	PrintStats("raven.storage.sstables");
				//break;
				default:
					throw new NotSupportedException("Unknown benchmark: " + benchmark);
			}

			return parameters;
		}

		private Task<BenchmarkResult> SnappyUncompress(BenchmarkParameters parameters)
		{
			throw new NotImplementedException();
		}

		private Task<BenchmarkResult> SnappyCompress(BenchmarkParameters parameters)
		{
			throw new NotImplementedException();
		}

		private Task<BenchmarkResult> AcquireLoad(BenchmarkParameters parameters)
		{
			throw new NotImplementedException();
		}

		private Task<BenchmarkResult> Crc32c(BenchmarkParameters parameters)
		{
			const long Size = 4096;
			var buffer = new byte[Size];
			for (int i = 0; i < Size; i++)
			{
				buffer[i] = (byte)'x';
			}

			var result = new BenchmarkResult(parameters);

			long bytes = 0;
			uint crc = 0;
			while (bytes < 500 * 1048576)
			{
				crc = Crc.CalculateCrc(0, buffer, 0, buffer.Length);
				bytes += Size;
				result.FinishOperation();
			}

			result.AddBytes(bytes);
			result.AddMessage("(4K per op)");
			result.AddMessage(string.Format("CRC is {0}", crc));

			return new CompletedTask<BenchmarkResult>(result);
		}

		private async Task<BenchmarkResult> Compact(BenchmarkParameters parameters)
		{
			var result = new BenchmarkResult(parameters);
			await storage.Commands.CompactRangeAsync(null, null);
			return result;
		}

		private Task<BenchmarkResult> ReadWhileWriting(BenchmarkParameters parameters)
		{
			var random = new Random();
			var generator = new RandomGenerator();

			var readTask = ReadRandom(parameters);
			Task.Factory.StartNew(
				async () =>
				{
					while (readTask.IsCompleted == false)
					{
						var batch = new WriteBatch();

						var k = random.Next() % options.Num;
						var key = string.Format("{0:0000000000000000}", k);

						batch.Put(key, generator.Generate(parameters.ValueSize));
						await storage.Writer.WriteAsync(batch);
					}
				});

			return readTask;
		}

		private Task<BenchmarkResult> DeleteRandom(BenchmarkParameters parameters)
		{
			return DoDelete(parameters, false);
		}

		private Task<BenchmarkResult> DeleteSeq(BenchmarkParameters parameters)
		{
			return DoDelete(parameters, true);
		}

		private async Task<BenchmarkResult> DoDelete(BenchmarkParameters parameters, bool seq)
		{
			var random = new Random();
			var result = new BenchmarkResult(parameters);

			for (var i = 0; i < parameters.Num; i += parameters.EntriesPerBatch)
			{
				var batch = new WriteBatch();
				for (var j = 0; j < parameters.EntriesPerBatch; j++)
				{
					var k = seq ? i + j : random.Next() % options.Num;
					var key = string.Format("{0:0000000000000000}", k);
					batch.Delete(key);
					result.FinishOperation();
				}

				await storage.Writer.WriteAsync(batch);
			}

			return (result);
		}

		private async Task<BenchmarkResult> ReadHot(BenchmarkParameters parameters)
		{
			var random = new Random();
			var range = (options.Num + 99) / 100;

			var result = new BenchmarkResult(parameters);

			for (var i = 0; i < parameters.Reads; i++)
			{
				var k = random.Next() % range;
				var key = string.Format("{0:0000000000000000}", k);
				storage.Reader.Read(key);
				result.FinishOperation();
			}

			return result;
		}

		private async Task<BenchmarkResult> SeekRandom(BenchmarkParameters parameters)
		{
			var random = new Random();
			var found = 0;

			var result = new BenchmarkResult(parameters);

			for (var i = 0; i < parameters.Reads; i++)
			{
				using (var iterator = await storage.Reader.NewIteratorAsync(new ReadOptions()))
				{
					var k = random.Next() % options.Num;
					var key = string.Format("{0:0000000000000000}", k);
					Slice sliceKey = key;

					iterator.Seek(sliceKey);
					if (iterator.IsValid && sliceKey.CompareTo(iterator.Key) == 0)
						found++;

					result.FinishOperation();
				}
			}

			result.AddMessage(string.Format("({0} of {1} found)", found, parameters.Num));

			return result;
		}

		private async Task<BenchmarkResult> ReadMissing(BenchmarkParameters parameters)
		{
			var random = new Random();
			var result = new BenchmarkResult(parameters);

			for (int i = 0; i < parameters.Reads; i++)
			{
				var k = random.Next() % options.Num;
				var key = string.Format("{0:0000000000000000}", k);

				storage.Reader.Read(key);
				result.FinishOperation();
			}

			return result;
		}

		private async Task<BenchmarkResult> ReadRandom(BenchmarkParameters parameters)
		{
			var random = new Random();
			var found = 0;

			var result = new BenchmarkResult(parameters);

			for (int i = 0; i < parameters.Reads; i++)
			{
				var k = random.Next() % options.Num;
				var key = string.Format("{0:0000000000000000}", k);

				if (storage.Reader.Read(key) != null)
					found++;

				result.FinishOperation();
			}

			result.AddMessage(string.Format("({0} of {1} found)", found, parameters.Num));

			return result;
		}

		private async Task<BenchmarkResult> ReadReverse(BenchmarkParameters parameters)
		{
			var result = new BenchmarkResult(parameters);
			using (var iterator = await storage.Reader.NewIteratorAsync(new ReadOptions()))
			{
				var i = 0;
				long bytes = 0;
				for (iterator.SeekToLast(); i < parameters.Reads && iterator.IsValid; iterator.Prev())
				{
					bytes += iterator.Key.Count + iterator.CreateValueStream().Length;
					result.FinishOperation();
					++i;
				}

				result.AddBytes(bytes);
				return result;
			}
		}

		private async Task<BenchmarkResult> ReadSequential(BenchmarkParameters parameters)
		{
			var result = new BenchmarkResult(parameters);
			using (var iterator = await storage.Reader.NewIteratorAsync(new ReadOptions()))
			{
				var i = 0;
				long bytes = 0;
				for (iterator.SeekToFirst(); i < parameters.Reads && iterator.IsValid; iterator.Next())
				{
					bytes += iterator.Key.Count + iterator.CreateValueStream().Length;
					result.FinishOperation();
					++i;
				}

				result.AddBytes(bytes);
				return result;
			}
		}

		private Task<BenchmarkResult> WriteRandom(BenchmarkParameters parameters)
		{
			return DoWrite(parameters, false);
		}

		private Task<BenchmarkResult> WriteSeq(BenchmarkParameters parameters)
		{
			return DoWrite(parameters, true);
		}

		private async Task<BenchmarkResult> DoWrite(BenchmarkParameters parameters, bool seq)
		{
			var random = new Random();
			var generator = new RandomGenerator();

			var result = new BenchmarkResult(parameters);

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

				await storage.Writer.WriteAsync(batch);
			}

			result.AddBytes(bytes);

			return result;
		}

		private async Task OpenAsync()
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
			await storage.InitAsync();
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