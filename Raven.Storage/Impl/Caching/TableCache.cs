using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Runtime.Caching;
using Raven.Storage.Comparing;
using Raven.Storage.Data;
using Raven.Storage.Memory;
using Raven.Storage.Reading;
using Raven.Storage.Util;
using Raven.Temp.Logging;

namespace Raven.Storage.Impl.Caching
{
	using System.Threading;

	public class TableCache : IDisposable
	{
		private ILog log = LogManager.GetCurrentClassLogger();

		private readonly ReaderWriterLockSlim slim = new ReaderWriterLockSlim();

		private readonly StorageState state;
		private LruCache<ulong, Table> cache;

		public TableCache(StorageState state)
		{
			this.state = state;
			cache = new LruCache<ulong, Table>(state.Options.MaxTablesCacheSize);
		}

		public IIterator NewIterator(ReadOptions options, ulong fileNumber, long fileSize)
		{
			try
			{
				var table = FindTable(fileNumber, fileSize);

				return table.CreateIterator(options);
			}
			catch (Exception e)
			{
				log.InfoException("Could not open iterator for " + fileNumber + ", will return empty iterator", e);
				return new EmptyIterator();
			}
		}

		private Table FindTable(ulong fileNumber, long fileSize)
		{
			slim.EnterReadLock();
			try
			{
				Table table;
				if (cache.TryGet(fileNumber, out table))
				{
					return table;
				}
			}
			finally
			{
				slim.ExitReadLock();
			}

			slim.EnterWriteLock();
			try
			{
				Table table;
				if (cache.TryGet(fileNumber, out table))
				{
					return table;
				}

				var filePath = state.FileSystem.GetFullFileName(fileNumber, Constants.Files.Extensions.TableFile);

				IAccessor file = state.FileSystem.OpenMemoryMap(filePath);
				var fileData = new FileData(file, fileSize);
				table = new Table(state, fileData);

				cache.Set(fileNumber, table);

				return table;
			}
			finally
			{
				slim.ExitWriteLock();
			}

		}


		public void Evict(ulong fileNumber)
		{
			cache.Remove(fileNumber);
		}

		public ItemState Get(InternalKey key, ulong fileNumber, long fileSize, ReadOptions readOptions, IComparator comparator,
							 out Stream stream)
		{
			Table table = FindTable(fileNumber, fileSize);

			Tuple<Slice, Stream> result = table.InternalGet(readOptions, key);

			stream = null;

			if (result == null)
			{
				return ItemState.NotFound;
			}

			bool shouldDispose = true;
			try
			{
				InternalKey internalKey;
				if (!InternalKey.TryParse(result.Item1, out internalKey))
				{
					return ItemState.Corrupt;
				}

				if (comparator.Compare(internalKey.UserKey, key.UserKey) == 0)
				{
					bool isFound = internalKey.Type == ItemType.Value;
					if (!isFound)
					{
						return ItemState.Deleted;
					}

					stream = result.Item2;
					shouldDispose = false;
					return ItemState.Found;
				}

				return ItemState.NotFound;
			}
			finally
			{
				if (shouldDispose && result.Item2 != null)
					result.Item2.Dispose();
			}
		}

		public void Dispose()
		{
			cache.Dispose();
		}
	}

	public enum ItemState
	{
		NotFound = 1,
		Found = 2,
		Deleted = 3,
		Corrupt = 4
	}
}