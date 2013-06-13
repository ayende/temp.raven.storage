using System;
using System.Globalization;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Runtime.Caching;
using Raven.Abstractions.Logging;
using Raven.Storage.Comparing;
using Raven.Storage.Data;
using Raven.Storage.Memory;
using Raven.Storage.Reading;

namespace Raven.Storage.Impl.Caching
{
	public class TableCache
	{
		private ILog log = LogManager.GetCurrentClassLogger();
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

			var o = state.Options.TableCache.Get(key);
			if (o != null)
			{
				return (TableAndFile) o;
			}

			string filePath = state.FileSystem.GetFullFileName(
				state.DatabaseName, fileNumber, Constants.Files.Extensions.TableFile);

			MemoryMappedFile file = MemoryMappedFile.CreateFromFile(filePath, FileMode.Open);
			var fileData = new FileData(new MemoryMappedFileAccessor(file), fileSize);
			var table = new Table(state.Options, fileData);

			var tableAndFile = new TableAndFile(fileData, table);

			state.Options.TableCache.Add(key, tableAndFile, new CacheItemPolicy
				{
					RemovedCallback = CacheRemovedCallback
				});

			return tableAndFile;
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

		public ItemState Get(Slice key, ulong fileNumber, long fileSize, ReadOptions readOptions, IComparator comparator,
		                     out Stream stream)
		{
			TableAndFile tableAndFile = FindTable(fileNumber, fileSize);

			Tuple<Slice, Stream> result = tableAndFile.Table.InternalGet(readOptions, key);

			stream = null;

			if (result == null)
			{
				return ItemState.NotFound;
			}

			InternalKey internalKey;
			if (!InternalKey.TryParse(result.Item1, out internalKey))
			{
				return ItemState.Corrupt;
			}

			if (comparator.Compare(internalKey.UserKey, key) == 0)
			{
				bool isFound = internalKey.Type == ItemType.Value;
				if (!isFound)
				{
					return ItemState.Deleted;
				}

				stream = result.Item2;
				return ItemState.Found;
			}

			return ItemState.NotFound;
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