namespace Raven.Storage.Benchmark
{
	using System;
	using System.Threading.Tasks;

	using NDesk.Options;

	using NLog;

	public class Program
	{
		private readonly BenchmarkOptions options;

		private readonly OptionSet optionSet;

		private bool enableLogging = false;

		private Program()
		{
			options = new BenchmarkOptions();
			optionSet = new OptionSet
				            {
					            {
						            "benchmarks:",
						            "Comma-separated list of operations to run in the specified order" 
									+ Environment.NewLine + "Actual benchmarks:" 
									+ Environment.NewLine + "- fillseq - write N values in sequential key order in async mode"
						            + Environment.NewLine + "- fillrandom - write N values in random key order in async mode"
						            + Environment.NewLine + "- overwrite - overwrite N values in random key order in async mode"
						            + Environment.NewLine + "- fillsync - write N/100 values in random key order in sync mode"
						            + Environment.NewLine + "- fill100K - write N/1000 100K values in random order in async mode"
						            + Environment.NewLine + "- deleteseq - delete N keys in sequential order"
						            + Environment.NewLine + "- deleterandom - delete N keys in random order"
						            + Environment.NewLine + "- readseq - read N times sequentially"
						            + Environment.NewLine + "- readreverse - read N times in reverse order"
						            + Environment.NewLine + "- readrandom - read N times in random order"
						            + Environment.NewLine + "- readmissing - read N missing keys in random order"
						            + Environment.NewLine + "- readhot - read N times in random order from 1% section of DB"
						            + Environment.NewLine + "- seekrandom - N random seeks"
									+ Environment.NewLine + "- crc32c - repeated crc32c of 4K of data" 
									+ Environment.NewLine + "- acquireload - load N*1000 times"
									+ Environment.NewLine + "Meta operations:"
						            + Environment.NewLine + "- compact - Compact the entire DB"
									+ Environment.NewLine + "- stats - Print DB stats"
									+ Environment.NewLine + "- sstables - Print sstable info"
									+ Environment.NewLine + "- heapprofile - Dump a heap profile (if supported by this port)",
						            s => options.Benchmarks = s.Split(',')
					            },
					            { "histogram:", "Print histogram of operation timings", s => options.Histogram = bool.Parse(s) },
					            { "use-existing-db:", "If true, do not destroy the existing database. If you set this flag and also specify a benchmark that wants a fresh database, that benchmark will fail.", s => options.UseExistingDatabase = bool.Parse(s) },
					            { "num:", "Number of key/values to place in database", s => options.Num = int.Parse(s) },
					            { "reads:", "Number of read operations to do. If negative, do FLAGS_num reads.", s => options.Reads = int.Parse(s) },
					            { "threads:", "Number of concurrent threads to run.", s => options.Threads = int.Parse(s) },
					            { "value-size:", "Size of each value", s => options.ValueSize = int.Parse(s) },
					            { "write-batch-size:", "Number of bytes to buffer in memtable before compacting", s => options.WriteBatchSize = int.Parse(s) },
					            { "cache-size:", "Number of megabytes to use as a cache of uncompressed data. Negative means use default settings.", s => options.CacheSize = int.Parse(s) },
					            { "bloom-bits:", "Bloom filter bits per key. Negative means use default settings.", s => options.BloomBits = int.Parse(s) },
					            { "db:", "Use the db with the following name.", s => options.DatabaseName = s },
								{ "log:", "Enables logging.", s => enableLogging = true },
					            { "h|?|help", v => PrintUsageAndExit(0) },
				            };
		}

		public static void Main(string[] args)
		{
			var program = new Program();
			program.ParseAsync(args).Wait();
		}

		private async Task ParseAsync(string[] args)
		{
			// Do these arguments the traditional way to maintain compatibility
			//if (args.Length < 3)
			//	PrintUsageAndExit(-1);

			try
			{
				optionSet.Parse(args);
			}
			catch (Exception e)
			{
				PrintUsageAndExit(e);
			}

			if (string.IsNullOrEmpty(options.DatabaseName))
			{
				options.DatabaseName = string.Format("DBBench-{0}-{1}", DateTime.Now.ToString("yyyy-MM-dd,HH-mm-ss"), Guid.NewGuid());
			}

			ConfigureLogging();

			try
			{
				using (var benchmark = new Benchmark(options, Console.WriteLine))
				{
					await benchmark.RunAsync();
				}
			}
			catch (Exception e)
			{
				PrintUsageAndExit(e);
			}
		}

		private void PrintUsageAndExit(Exception e)
		{
			Console.WriteLine(e);
			PrintUsageAndExit(-1);
		}

		private void PrintUsageAndExit(int exitCode)
		{
			Console.WriteLine(@"
Benchmarking utility for Raven Storage
----------------------------------------
Copyright (C) 2008 - {0} - Hibernating Rhinos
----------------------------------------
Command line options:", DateTime.UtcNow.Year);

			optionSet.WriteOptionDescriptions(Console.Out);
			Console.WriteLine();

			Environment.Exit(exitCode);
		}

		private void ConfigureLogging()
		{
			if (!enableLogging)
				LogManager.DisableLogging();
		}
	}
}
