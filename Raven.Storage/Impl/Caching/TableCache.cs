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
		private LruCache<ulong,TableAndFile> _cache;

		public TableCache(StorageState state)
		{
			this.state = state;
			_cache = new LruCache<ulong, TableAndFile>(state.Options.MaxTablesCacheSize);
		}

		public IIterator NewIterator(ReadOptions options, ulong fileNumber, long fileSize)
		{
			try
			{
				var tableAndFile = FindTable(fileNumber, fileSize);

				return tableAndFile.Table.CreateIterator(options);
			}
			catch (Exception e)
			{
				log.InfoException("Could not open iterator for " + fileNumber + ", will return empty iterator", e);
				return new EmptyIterator();
			}
		}

		private TableAndFile FindTable(ulong fileNumber, long fileSize)
		{
			slim.EnterReadLock();
			try
			{
				TableAndFile file;
				if (_cache.TryGet(fileNumber, out file))
				{
					return file;
				}
			}
			finally
			{
				slim.ExitReadLock();
			}

			slim.EnterWriteLock();
			try
			{
				TableAndFile value;
				if (_cache.TryGet(fileNumber, out value))
				{
					return value;
				}

				var filePath = state.FileSystem.GetFullFileName(fileNumber, Constants.Files.Extensions.TableFile);

				IAccessor file = state.FileSystem.OpenMemoryMap(filePath);
				var fileData = new FileData(file, fileSize);
				var table = new Table(state, fileData);

				var tableAndFile = new TableAndFile(fileData, table);

				_cache.Set(fileNumber, tableAndFile);

				return tableAndFile;
			}
			finally
			{
				slim.ExitWriteLock();
			}

		}


		public void Evict(ulong fileNumber)
		{
			_cache.Remove(fileNumber);
		}

		public ItemState Get(InternalKey key, ulong fileNumber, long fileSize, ReadOptions readOptions, IComparator comparator,
							 out Stream stream)
		{
			TableAndFile tableAndFile = FindTable(fileNumber, fileSize);

			Tuple<Slice, Stream> result = tableAndFile.Table.InternalGet(readOptions, key);

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
			_cache.Dispose();
		}
	}

	public enum ItemState
	{
		NotFound = 1,
		Found = 2,
		Deleted = 3,
		Corrupt = 4
	}

	internal class TableAndFile : IDisposable
	{
		public TableAndFile(FileData fileData, Table table)
		{
			FileData = fileData;
			Table = table;
		}

		public FileData FileData { get; private set; }

		public Table Table { get; private set; }

		public void Dispose()
		{
			if (Table != null)
				Table.Dispose();

			if (FileData != null && FileData.File != null)
				FileData.File.Dispose();
		}
	}
}