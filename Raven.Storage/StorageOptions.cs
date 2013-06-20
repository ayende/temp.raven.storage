using System;
using Raven.Storage.Comparing;
using Raven.Storage.Filtering;
using Raven.Storage.Util;

namespace Raven.Storage
{
	public class StorageOptions : IDisposable
	{
		/// <summary>
		/// The maximum size of key size (in bytes) that we expect
		/// Using keys bigger than this value is going to consume a lot more memory
		/// 
		/// Default: 2KB
		/// </summary>
		public int MaximumExpectedKeySize { get; set; }

		/// <summary>
		/// This is used to define the order of keys in the database.
		/// Default: a comparator that uses case insensitive character matches.
		/// 
		/// REQUIRED: The database must always be opened using the same comparator that
		/// created it.
		/// </summary>
		public IComparator Comparator { get; set; }

		/// <summary>
		/// If set to a non null value, will use the specified filter policy to reduce 
		/// disk read. 
		/// </summary>
		public IFilterPolicy FilterPolicy { get; set; }

		/// <summary>
		/// Approximate size of user data packed per block.
		/// 
		/// Default: 4Kb
		/// </summary>
		public int BlockSize { get; set; }

		/// <summary>
		/// Max size of the number of blocks cached per table
		/// Default: 2048
		/// </summary>
		public int MaxBlockCacheSizePerTableFile { get; set; }

		/// <summary>
		/// Max size of the number of tables cached 
		/// Default: 2048
		/// </summary>
		public int MaxTablesCacheSize{ get; set; }

		/// <summary>
		///  Amount of data to build up in memory (backed by an unsorted log
		///  on disk) before converting to a sorted on-disk file.
		/// 
		///  Larger values increase performance, especially during bulk loads.
		///  Up to two write buffers may be held in memory at the same time,
		///  so you may wish to adjust this parameter to control memory usage.
		///  Also, a larger write buffer will result in a longer recovery time
		///  the next time the database is opened.
		/// 
		///  Default: 4MB
		/// </summary>
		public int WriteBatchSize { get; set; }

		/// <summary>
		/// Number of keys between restart points for delta encoding of keys.
		/// 
		/// Most clients should leave this param alone.
		/// 
		/// Default: 16
		/// </summary>
		public int BlockRestartInterval { get; set; }

		/// <summary>
		/// If true, the database will be created it if doesn't exists
		/// Default: true
		/// </summary>
		public bool CreateIfMissing { get; set; }

		/// <summary>
		/// If true, the database will error if the database already exists
		/// Default: false
		/// </summary>
		public bool ErrorIfExists { get; set; }

		/// <summary>
		/// Default: 256
		/// </summary>
		public int CacheSizeInMegabytes { get; set; }

		/// <summary>
		/// If true, the database iwll do aggressive checking of the data it is process and will 
		/// fail early if it detects any errors.
		/// This may cause a corruption of a single entry to cause the etnire database become inoperable.
		/// Default: false
		/// </summary>
		public bool ParanoidChecks { get; set; }

		

		/// <summary>
		/// The buffer pool to use
		/// </summary>
		public BufferPool BufferPool { get; set; }

		public StorageOptions()
		{
			CreateIfMissing = true;
			BlockSize = 1024 * 4;
			MaxBlockCacheSizePerTableFile = 2048;
			MaxTablesCacheSize = 2048;
			BlockRestartInterval = 16;
			Comparator = new CaseInsensitiveComparator();
			FilterPolicy = new BloomFilterPolicy();
			MaximumExpectedKeySize = 2048;
			WriteBatchSize = 1024 * 1024 * 4;
			CacheSizeInMegabytes = 256;
			BufferPool = new BufferPool();
		}

		public void Dispose()
		{
			
		}
	}
}