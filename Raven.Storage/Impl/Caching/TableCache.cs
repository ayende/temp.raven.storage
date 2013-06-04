namespace Raven.Storage.Impl.Caching
{
	using System;
	using System.Globalization;
	using System.IO;
	using System.IO.MemoryMappedFiles;
	using System.Runtime.Caching;

	using Raven.Storage.Comparing;
	using Raven.Storage.Data;
	using Raven.Storage.Memory;
	using Raven.Storage.Reading;

	public class TableCache
	{
		private readonly StorageState state;

		public TableCache(StorageState state)
		{
			this.state = state;
		}

		public IIterator NewIterator(ReadOptions options, ulong fileNumber, long fileSize)
		{
			try
			{
				var tableAndFile = this.FindTable(fileNumber, fileSize);

				return tableAndFile.Table.CreateIterator(options);
			}
			catch
			{
				return new EmptyIterator();
			}
		}

		private TableAndFile FindTable(ulong fileNumber, long fileSize)
		{
			var key = fileNumber.ToString(CultureInfo.InvariantCulture);

			if (this.state.Options.TableCache.Contains(key))
			{
				return (TableAndFile)this.state.Options.TableCache.Get(key);
			}

			var fileName = this.state.FileSystem.GetFileName(
				this.state.DatabaseName, fileNumber, Constants.Files.Extensions.TableFile);

			var file = MemoryMappedFile.CreateFromFile(fileName, FileMode.Open);
			var fileData = new FileData(new MemoryMappedFileAccessor(file), fileSize);
			var table = new Table(this.state.Options, fileData);

			var tableAndFile = new TableAndFile(fileData, table);

			this.state.Options.TableCache.Add(key, tableAndFile, new CacheItemPolicy
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
			var key = fileNumber.ToString(CultureInfo.InvariantCulture);
			if (state.Options.TableCache.Contains(key))
			{
				state.Options.TableCache.Remove(key);
			}
		}

		public ItemState Get(Slice key, ulong fileNumber, long fileSize, ReadOptions readOptions, IComparator comparator, out Stream stream)
		{
			var tableAndFile = FindTable(fileNumber, fileSize);

			var result = tableAndFile.Table.InternalGet(readOptions, key);

			stream = null;

			if (result == null)
			{
				return ItemState.NotFound;
			}

			ParsedInternalKey internalKey;
			if (!ParsedInternalKey.TryParseInternalKey(result.Item1, out internalKey))
			{
				return ItemState.Corrupt;
			}

			if (comparator.Compare(internalKey.UserKey, key) == 0)
			{
				var isFound = internalKey.Type == ItemType.Value;
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
		public FileData FileData { get; private set; }

		public Table Table { get; private set; }

		public TableAndFile(FileData fileData, Table table)
		{
			this.FileData = fileData;
			this.Table = table;
		}

		public void Dispose()
		{
			if (this.Table != null)
				this.Table.Dispose();

			if (this.FileData != null)
				this.FileData.File.Dispose();
		}
	}
}