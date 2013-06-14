﻿using System;
using System.Globalization;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Runtime.Caching;
using Raven.Abstractions.Logging;
using Raven.Storage.Comparing;
using Raven.Storage.Data;
using Raven.Storage.Memory;
using Raven.Storage.Reading;
using Raven.Storage.Util;

namespace Raven.Storage.Impl.Caching
{
	using System.Threading;

	public class TableCache : IDisposable
	{
		private ILog log = LogManager.GetCurrentClassLogger();

		private readonly ReaderWriterLockSlim slim = new ReaderWriterLockSlim();

		private readonly StorageState state;

		public TableCache(StorageState state)
		{
			this.state = state;
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
			string key = fileNumber.ToString(CultureInfo.InvariantCulture);

			slim.EnterReadLock();
			try
			{
				var o = state.Options.TableCache.Get(key);
				if (o != null)
				{
					return (TableAndFile)o;
				}
			}
			finally
			{
				slim.ExitReadLock();
			}

			slim.EnterWriteLock();
			try
			{
				var o = state.Options.TableCache.Get(key);
				if (o != null)
				{
					return (TableAndFile)o;
				}

				var filePath = state.FileSystem.GetFullFileName(fileNumber, Constants.Files.Extensions.TableFile);

				IAccessor file = state.FileSystem.OpenMemoryMap(filePath);
				var fileData = new FileData(file, fileSize);
				var table = new Table(state, fileData);

				var tableAndFile = new TableAndFile(fileData, table);

				state.Options.TableCache.Add(key, tableAndFile, new CacheItemPolicy
				{
					RemovedCallback = CacheRemovedCallback
				});

				return tableAndFile;
			}
			finally
			{
				slim.ExitWriteLock();
			}

		}

		private static void CacheRemovedCallback(CacheEntryRemovedArguments arguments)
		{
			var disposable = arguments.CacheItem.Value as IDisposable;
			if (disposable == null)
				return;

			try
			{
				disposable.Dispose();
			}
			catch (Exception)
			{
				// we can't allow exception to escape from here, since this may be happening on the cache thread
			}
		}

		public void Evict(ulong fileNumber)
		{
			string key = fileNumber.ToString(CultureInfo.InvariantCulture);
			if (state.Options.TableCache.Contains(key))
			{
				state.Options.TableCache.Remove(key);
			}
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
			foreach (var item in state.Options.BlockCache)
			{
				var block = item.Value as Block;
				if (block != null)
				{
					block.Dispose();
				}
			}
			foreach (var item in state.Options.TableCache)
			{
				var tableAndFile = item.Value as TableAndFile;
				if (tableAndFile != null)
				{
					if (tableAndFile.FileData != null && tableAndFile.FileData.File != null)
						tableAndFile.FileData.File.Dispose();
					if (tableAndFile.Table != null)
						tableAndFile.Table.Dispose();
				}
			}
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