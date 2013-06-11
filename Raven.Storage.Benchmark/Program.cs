namespace Raven.Storage.Benchmark
{
	using System;
	using System.Xml;

	using NDesk.Options;

	using NLog.Config;

	public class Program
	{
		private readonly BenchmarkOptions options;

		private readonly OptionSet optionSet;

		private Program()
		{
			ConfigureLogging();

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
					            {
						            "compression-ratio:",
						            "Arrange to generate values that shrink to this fraction of their original size after compression"
						            , s => options.CompressionRatio = double.Parse(s)
					            },
					            { "histogram:", "Print histogram of operation timings", s => options.Histogram = bool.Parse(s) },
					            { "use-existing-db:", "If true, do not destroy the existing database. If you set this flag and also specify a benchmark that wants a fresh database, that benchmark will fail.", s => options.UseExistingDatabase = bool.Parse(s) },
					            { "num:", "Number of key/values to place in database", s => options.Num = int.Parse(s) },
					            { "reads:", "Number of read operations to do. If negative, do FLAGS_num reads.", s => options.Reads = int.Parse(s) },
					            { "threads:", "Number of concurrent threads to run.", s => options.Threads = int.Parse(s) },
					            { "value-size:", "Size of each value", s => options.ValueSize = int.Parse(s) },
					            { "write-batch-size:", "Number of bytes to buffer in memtable before compacting", s => options.WriteBatchSize = int.Parse(s) },
					            { "cache-size:", "Number of bytes to use as a cache of uncompressed data. Negative means use default settings.", s => options.CacheSize = int.Parse(s) },
					            { "bloom-bits:", "Bloom filter bits per key. Negative means use default settings.", s => options.BloomBits = int.Parse(s) },
					            { "open-files:", "Maximum number of files to keep open at the same time (use default if == 0)", s => options.OpenFiles = int.Parse(s) },
					            { "db:", "Use the db with the following name.", s => options.DatabaseName = s },
					            { "h|?|help", v => PrintUsageAndExit(0) },
				            };
		}

		public static void Main(string[] args)
		{
			var program = new Program();
			program.Parse(args);
		}

		private void Parse(string[] args)
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
				options.DatabaseName = string.Format("DBBench-{0}", DateTime.Now.ToString("yyyy-MM-dd,HH-mm-ss"));
			}

			try
			{
				using (var benchmark = new Benchmark(options, Console.WriteLine))
				{
					benchmark.Run();
				}
			}
			catch (Exception e)
			{
				PrintUsageAndExit(e);
			}
		}

		private void PrintUsageAndExit(Exception e)
		{
			Console.WriteLine(e.Message);
			PrintUsageAndExit(-1);
		}

		private void PrintUsageAndExit(int exitCode)
		{
			Console.WriteLine(@"
Benchmarking utility for Raven Storage
----------------------------------------
Copyright (C) 2008 - {0} - Hibernating Rhinos
----------------------------------------
Usage:
	- Import the dump.raven file to a local instance:
		Raven.Smuggler in http://localhost:8080/ dump.raven
	- Export a local instance to dump.raven:
		Raven.Smuggler out http://localhost:8080/ dump.raven

Command line options:", DateTime.UtcNow.Year);

			optionSet.WriteOptionDescriptions(Console.Out);
			Console.WriteLine();

			Environment.Exit(exitCode);
		}

		private void ConfigureLogging()
		{
			using (var stream = this.GetType().Assembly.GetManifestResourceStream("Raven.Storage.Benchmark.NLog.config"))
			using (var reader = XmlReader.Create(stream))
			{
				NLog.LogManager.Configuration = new XmlLoggingConfiguration(reader, "default-config");
			}
		}
	}
}
