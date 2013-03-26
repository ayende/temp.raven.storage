using System;
using System.IO;
using System.Runtime.Caching;
using Raven.Storage.Comparing;
using Raven.Storage.Data;
using Raven.Storage.Exceptions;
using Raven.Storage.Filtering;

namespace Raven.Storage.Reading
{
	public class Table : IDisposable
	{
		private readonly FileData _fileData;

		private readonly StorageOptions _storageOptions;
		private readonly Block _indexBlock;
		private readonly IFilter _filter;

		public Table(
			StorageOptions storageOptions,
			FileData fileData)
		{
			_fileData = fileData;
			try
			{
				_storageOptions = storageOptions;

				if (fileData.Size < Footer.EncodedLength)
					throw new CorruptedDataException("File is too short to be an sstable");

				var footer = new Footer();
				using (var accessor = fileData.File.CreateViewAccessor(fileData.Size - Footer.EncodedLength, Footer.EncodedLength))
				{
					footer.DecodeFrom(accessor);
				}

				var readOptions = new ReadOptions
					{
						VerifyChecksums = _storageOptions.ParanoidChecks
					};
				_indexBlock = new Block(_storageOptions, readOptions, footer.IndexHandle, fileData);

				if (_storageOptions.FilterPolicy == null)
					return; // we don't need any metadata

				using (var metaBlock = new Block(_storageOptions, readOptions, footer.MetaIndexHandle, fileData))
				using (var iterator = metaBlock.CreateIterator(CaseInsensitiveComparator.Default))
				{
					var filterName = ("filter." + _storageOptions.FilterPolicy.Name);
					iterator.Seek(filterName);
					if (iterator.IsValid && CaseInsensitiveComparator.Default.Compare(filterName, iterator.Key) == 0)
					{
						_filter = null;
						throw new NotSupportedException("Reading filters hasn't been ported yet");
					}
				}
			}
			catch (Exception)
			{
				Dispose();
				throw;
			}
		}

		/// <summary>
		/// Returns a new iterator over the table contents.
		/// The result of NewIterator() is initially invalid (caller must
		/// call one of the Seek methods on the iterator before using it).
		/// </summary>
		public IIterator CreateIterator(ReadOptions readOptions)
		{
			return new TwoLevelIterator(_indexBlock.CreateIterator(_storageOptions.Comparator), this, readOptions);
		}

		private Tuple<Slice, Stream> InternalGet(ReadOptions readOptions, Slice key)
		{
			using (var iterator = _indexBlock.CreateIterator(_storageOptions.Comparator))
			{
				iterator.Seek(key);
				if (iterator.IsValid == false)
					return null;
				var handle = new BlockHandle();
				using (var stream = iterator.CreateValueStream())
				{
					handle.DecodeFrom(stream);
				}
				if (_filter != null && _filter.KeyMayMatch(handle.Position, key) == false)
				{
					return null; // opptimized not found by filter, no need to read the actual block
				}
				using (var blockIterator = CreateBlockIterator(handle, readOptions))
				{
					blockIterator.Seek(key);
					if (blockIterator.IsValid == false)
						return null;
					return Tuple.Create(blockIterator.Key, blockIterator.CreateValueStream());
				}
			}
		}

		internal IIterator CreateBlockIterator(BlockHandle handle, ReadOptions readOptions)
		{
			var blockCache = _storageOptions.BlockCache;

			if (blockCache == null)
			{
				Block uncachedBlock = null;
				IIterator blockIterator = null;
				try
				{
					uncachedBlock = new Block(_storageOptions, readOptions, handle, _fileData);
					// uncachedBlock.InrementUsage(); - intentionally not calling this, will be disposed when the iterator is disposed
					blockIterator = uncachedBlock.CreateIterator(_storageOptions.Comparator);
					return blockIterator;
				}
				catch (Exception)
				{
					if (uncachedBlock != null)
						uncachedBlock.Dispose();
					if (blockIterator != null)
						blockIterator.Dispose();
					throw;
				}
			}

			var cacheKey = handle.CacheKey;
			var cachedBlock = blockCache.Get(cacheKey) as Block;
			if (cachedBlock != null)
				return cachedBlock.CreateIterator(_storageOptions.Comparator);
			var block = new Block(_storageOptions, readOptions, handle, _fileData);
			block.InrementUsage(); // the cache is using this, so avoid having it disposed by the cache while in use
			blockCache.Set(cacheKey, block, new CacheItemPolicy
				{
					RemovedCallback = CacheRemovedCallback
				});
			return block.CreateIterator(_storageOptions.Comparator);
		}

		private void CacheRemovedCallback(CacheEntryRemovedArguments arguments)
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

		public void Dispose()
		{
			if (_fileData != null && _fileData.File != null)
				_fileData.File.Dispose();
			if (_indexBlock != null)
				_indexBlock.Dispose();
		}
	}
}