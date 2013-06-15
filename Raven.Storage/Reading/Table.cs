using System;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Runtime.Caching;
using Raven.Storage.Comparing;
using Raven.Storage.Data;
using Raven.Storage.Exceptions;
using Raven.Storage.Filtering;
using Raven.Storage.Impl;

namespace Raven.Storage.Reading
{
	public class Table : IDisposable
	{
		private readonly StorageState _storageState;
		private readonly FileData _fileData;
		private readonly Block _indexBlock;
		private readonly IFilter _filter;

		public Table(StorageState storageState, FileData fileData)
		{
			_storageState = storageState;
			try
			{
				_fileData = fileData;

				if (fileData.Size < Footer.EncodedLength)
					throw new CorruptedDataException("File is too short to be an sstable");

				var footer = new Footer();
				using (var accessor = fileData.File.CreateAccessor(fileData.Size - Footer.EncodedLength, Footer.EncodedLength))
				{
					footer.DecodeFrom(accessor);
				}

				var readOptions = new ReadOptions
					{
						VerifyChecksums = _storageState.Options.ParanoidChecks
					};
				_indexBlock = new Block(_storageState.Options, readOptions, footer.IndexHandle, fileData);
				_indexBlock.IncrementUsage();
				if (_storageState.Options.FilterPolicy == null)
					return; // we don't need any metadata

				using (var metaBlock = new Block(_storageState.Options, readOptions, footer.MetaIndexHandle, fileData))
				using (var iterator = metaBlock.CreateIterator(CaseInsensitiveComparator.Default))
				{
					var filterName = ("filter." + _storageState.Options.FilterPolicy.Name);
					iterator.Seek(filterName);
					if (iterator.IsValid && CaseInsensitiveComparator.Default.Compare(filterName, iterator.Key) == 0)
					{
						var handle = new BlockHandle();
						using (var stream = iterator.CreateValueStream())
						{
							handle.DecodeFrom(stream);
						}
						var filterAccessor = _fileData.File.CreateAccessor(handle.Position, handle.Count);
						try
						{
							_filter = _storageState.Options.FilterPolicy.CreateFilter(filterAccessor);
						}
						catch (Exception)
						{
							if (_filter == null)
								filterAccessor.Dispose();
							else
								_filter.Dispose();
							throw;
						}
						
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
			return new TwoLevelIterator(_indexBlock.CreateIterator(_storageState.Options.Comparator), CreateBlockIterator, readOptions);
		}


		internal Tuple<Slice, Stream> InternalGet(ReadOptions readOptions, InternalKey key)
		{
			using (var iterator = _indexBlock.CreateIterator(_storageState.Options.Comparator))
			{
				iterator.Seek(key.TheInternalKey);
				if (iterator.IsValid == false)
					return null;
				var handle = new BlockHandle();
				using (var stream = iterator.CreateValueStream())
				{
					handle.DecodeFrom(stream);
				}
				if (_filter != null && _filter.KeyMayMatch(handle.Position, key.UserKey) == false)
				{
					return null; // opptimized not found by filter, no need to read the actual block
				}
				using (var blockIterator = CreateBlockIterator(readOptions, handle))
				{
					blockIterator.Seek(key.TheInternalKey);
					if (blockIterator.IsValid == false)
						return null;
					return Tuple.Create(blockIterator.Key, blockIterator.CreateValueStream());
				}
			}
		}

		internal IIterator CreateBlockIterator(ReadOptions readOptions, BlockHandle handle)
		{
			var blockCache = _storageState.Options.BlockCache;

			if (blockCache == null)
			{
				Block uncachedBlock = null;
				IIterator blockIterator = null;
				try
				{
					uncachedBlock = new Block(_storageState.Options, readOptions, handle, _fileData);
					// uncachedBlock.IncrementUsage(); - intentionally not calling this, will be disposed when the iterator is disposed
					blockIterator = uncachedBlock.CreateIterator(_storageState.InternalKeyComparator);
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
			{
				return cachedBlock.CreateIterator(_storageState.InternalKeyComparator);
			}
			var block = new Block(_storageState.Options, readOptions, handle, _fileData);
			block.IncrementUsage(); // the cache is using this, so avoid having it disposed by the cache while in use
			blockCache.Set(cacheKey, block, new CacheItemPolicy
				{
					RemovedCallback = CacheRemovedCallback
				});
			return block.CreateIterator(_storageState.InternalKeyComparator);
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
			if (_filter != null)
				_filter.Dispose();
		}
	}
}
