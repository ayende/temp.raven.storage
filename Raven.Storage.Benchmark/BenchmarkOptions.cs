namespace Raven.Storage.Benchmark
{
	using System.Collections.Generic;

	internal class BenchmarkOptions
	{
		public BenchmarkOptions()
		{
			var options = new StorageOptions();

			Num = 10000;
			Reads = -1;
			Threads = 1;
			ValueSize = 100;
			Histogram = false;
			WriteBatchSize = options.WriteBatchSize;
			CacheSize = -1;
			BloomBits = -1;
			UseExistingDatabase = false;
			DatabaseName = null;

			Benchmarks = new List<string>
				             {
					             "fillseq",
					             "fillsync",
					             "fillrandom",
					             "overwrite",
					             "readrandom",
					             "readrandom", // Extra run to allow previous compactions to quiesce
					             "readseq",
					             "readreverse",
					             "compact",
					             "readrandom",
					             "readseq",
					             "readreverse",
					             "fill100K",
					             "crc32c",
					             "acquireload"
				             };
		}

		/// <summary>
		///   Comma-separated list of operations to run in the specified order
		///   Actual benchmarks:
		///      fillseq       -- write N values in sequential key order in async mode
		///      fillrandom    -- write N values in random key order in async mode
		///      overwrite     -- overwrite N values in random key order in async mode
		///      fillsync      -- write N/100 values in random key order in sync mode
		///      fill100K      -- write N/1000 100K values in random order in async mode
		///      deleteseq     -- delete N keys in sequential order
		///      deleterandom  -- delete N keys in random order
		///      readseq       -- read N times sequentially
		///      readreverse   -- read N times in reverse order
		///      readrandom    -- read N times in random order
		///      readmissing   -- read N missing keys in random order
		///      readhot       -- read N times in random order from 1% section of DB
		///      seekrandom    -- N random seeks
		///      crc32c        -- repeated crc32c of 4K of data
		///      acquireload   -- load N*1000 times
		///   Meta operations:
		///      compact     -- Compact the entire DB
		///      stats       -- Print DB stats
		///      sstables    -- Print sstable info
		///      heapprofile -- Dump a heap profile (if supported by this port)
		/// </summary>
		public IList<string> Benchmarks { get; set; }

		/// <summary>
		/// Number of key/values to place in database
		/// </summary>
		public int Num { get; set; }

		/// <summary>
		/// Number of read operations to do.  If negative, do FLAGS_num reads.
		/// </summary>
		public int Reads { get; set; }

		/// <summary>
		/// Number of concurrent threads to run.
		/// </summary>
		public int Threads { get; set; }

		/// <summary>
		/// Size of each value
		/// </summary>
		public int ValueSize { get; set; }

		/// <summary>
		/// Print histogram of operation timings
		/// </summary>
		public bool Histogram { get; set; }

		/// <summary>
		/// Number of bytes to buffer in memtable before compacting
		/// </summary>
		public int WriteBatchSize { get; set; }

		/// <summary>
		/// Number of megabytes to use as a cache of uncompressed data.
		/// Negative means use default settings.
		/// </summary>
		public int CacheSize { get; set; }

		/// <summary>
		/// Bloom filter bits per key.
		/// Negative means use default settings.
		/// </summary>
		public int BloomBits { get; set; }

		/// <summary>
		/// If true, do not destroy the existing database.  If you set this
		/// flag and also specify a benchmark that wants a fresh database, that
		/// benchmark will fail.
		/// </summary>
		public bool UseExistingDatabase { get; set; }

		/// <summary>
		/// Use the db with the following name.
		/// </summary>
		public string DatabaseName { get; set; }
	}
}