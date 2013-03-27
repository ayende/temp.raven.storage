using System;
using System.Collections.Specialized;
using System.Runtime.Caching;
using Raven.Storage.Comparing;
using Raven.Storage.Filtering;
using Raven.Storage.Util;

namespace Raven.Storage
{
	public class StorageOptions
	{
		private ObjectCache _blockCache;
		private bool _blockCacheSet;

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
		/// If true, the database iwll do aggressive checking of the data it is process and will 
		/// fail early if it detects any errors.
		/// This may cause a corruption of a single entry to cause the etnire database become inoperable.
		/// Default: false
		/// </summary>
		public bool ParanoidChecks { get; set; }

		/// <summary>
		/// Control over blocks (user data is stored in a set of blocks, and
		/// a block is the unit of reading from disk).
		/// 
		/// If set, use the specified cache for blocks, or not use any, if set to null
		/// If not set, will use a default cache
		/// </summary>
		public ObjectCache BlockCache
		{
			get
			{
				if (_blockCache == null && _blockCacheSet == false)
				{
					_blockCache = new MemoryCache("Raven.Storage.Default", new NameValueCollection
						{
							{"physicalMemoryLimitPercentage", "10"},
							{"pollingInterval", "00:05:00"},
							{"cacheMemoryLimitMegabytes", "256"}
						});
					_blockCacheSet = true;
				}
				return _blockCache;
			}
			set
			{
				var disposable = _blockCache as IDisposable;
				if (disposable != null)
				{
					disposable.Dispose();
				}
				_blockCache = value;
			}
		}

		/// <summary>
		/// The buffer pool to use
		/// </summary>
		public BufferPool BufferPool { get; set; }

		public StorageOptions()
		{
			CreateIfMissing = true;
			BlockSize = 1024 * 4;
			BlockRestartInterval = 16;
			Comparator = new CaseInsensitiveComparator();
			FilterPolicy = new BloomFilterPolicy();
			MaximumExpectedKeySize = 2048;
			WriteBatchSize = 1024*1024*4;
			BufferPool = new BufferPool();
		}
	}
}