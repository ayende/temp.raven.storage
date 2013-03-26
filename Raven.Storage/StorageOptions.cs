using System;
using System.Collections.Specialized;
using System.Runtime.Caching;
using Raven.Storage.Comparing;
using Raven.Storage.Filtering;

namespace Raven.Storage
{
	public class StorageOptions
	{
		private ObjectCache _blockCache;

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
		/// If non-NULL, use the specified cache for blocks.
		/// If NULL, will use a default cache
		/// </summary>
		public System.Runtime.Caching.ObjectCache BlockCache
		{
			get
			{
				if (_blockCache == null)
				{
					_blockCache = new MemoryCache("Raven.Storage.Default", new NameValueCollection
						{
							{"physicalMemoryLimitPercentage", "10"},
							{"pollingInterval", "00:05:00"},
							{"cacheMemoryLimitMegabytes", "256"}
						});
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

		public StorageOptions()
		{
			CreateIfMissing = true;
			BlockSize = 1024 * 4;
			BlockRestartInterval = 16;
			Comparator = new CaseInsensitiveComparator();
			MaximumExpectedKeySize = 2048;
		}
	}
}