﻿using System;
using System.IO;
using Raven.Storage.Data;
using Raven.Storage.Filtering;
using Raven.Storage.Impl;
using Raven.Storage.Util;

namespace Raven.Storage.Building
{
	public class TableBuilder : IDisposable
	{
		private readonly TemporaryFiles _temporaryFiles;
		private readonly StorageState _storageState;
		private byte[] _scratchBuffer;
		private byte[] _lastKeyBuffer;
		private readonly Stream _dataStream;
		private readonly Stream _indexStream;
		private bool _closed;

		private Slice _lastKey;
		private readonly FilterBlockBuilder _filterBuilder;
		private int _numEntries;
		private readonly BlockBuilder _indexBlock;

		private BlockBuilder _dataBlock;

		private bool _pendingIndexEntry;  // if this is true, the data block is empty
		private BlockHandle _pendingHandle;
		private readonly long _originalIndexStreamPosition;
		private readonly Stream _filterBlockStream;

		/// <summary>
		/// Create a new table builder.
		/// - storageOptions define the options for the table buildup.
		/// - dataStream is where the data for the table will be written to.
		///		REQUIRES: Being able to read dataStream.Position 
		/// - tempStream is where temporary data is written to avoid holding too much in memory
		///     REQUIRES: Being able to read tempStream.Position AND change tempStream.Position
		/// </summary>
		public TableBuilder(StorageState storageState,
			Stream dataStream,
			TemporaryFiles temporaryFiles)
		{
			_temporaryFiles = temporaryFiles;
			try
			{
				_storageState = storageState;
				_dataStream = dataStream;
				_indexStream = temporaryFiles.Create();
				_originalIndexStreamPosition = _indexStream.Position;

				_lastKeyBuffer = _storageState.Options.BufferPool.Take(storageState.Options.MaximumExpectedKeySize);
				_scratchBuffer = _storageState.Options.BufferPool.Take(storageState.Options.MaximumExpectedKeySize);

				if (storageState.Options.FilterPolicy != null)
				{
					var filterBuilder = storageState.Options.FilterPolicy.CreateBuilder();
					_filterBlockStream = temporaryFiles.Create();
					_filterBuilder = new FilterBlockBuilder(_filterBlockStream, filterBuilder);
					_filterBuilder.StartBlock(0);
				}

				_indexBlock = new BlockBuilder(_indexStream, storageState, _storageState.InternalKeyComparator,
				                               blockRestartInterval: 1);
				_dataBlock = new BlockBuilder(_dataStream, storageState, _storageState.InternalKeyComparator, _storageState.Options.BlockRestartInterval);
			}
			catch (Exception)
			{
				Dispose();
				throw;
			}
		}

		public int NumEntries
		{
			get { return _numEntries; }
		}

		public long FileSize
		{
			get { return _dataStream.Position; }
		}

		/// <summary>
		/// Add the key and value to the table.
		/// </summary>
		public void Add(Slice key, Stream value)
		{
			if (_closed)
				throw new InvalidOperationException("Cannot add after the table builder was closed");

			if (_storageState.InternalKeyComparator.Compare(key, _lastKey) <= 0)
				throw new InvalidOperationException("Keys must be added in increasing order");

			if (_pendingIndexEntry)
			{
				if (_dataBlock.IsEmpty == false)
					throw new InvalidOperationException("Cannot have pending index entry with a non empty data block");

				_storageState.InternalKeyComparator.FindShortestSeparator(ref _lastKey, key);

				_indexBlock.Add(_lastKey, _pendingHandle.AsStream());

				_pendingIndexEntry = false;
			}

			if (_filterBuilder != null)
				_filterBuilder.Add(key);

			_numEntries++;
			_lastKey = new Slice(ref _lastKeyBuffer, key, _storageState.Options.BufferPool);
			
			_dataBlock.Add(key, value);

			if (_dataBlock.EstimatedSize >= _storageState.Options.BlockSize)
				Flush();
		}

		/// <summary>
		/// Advance: Forces the creation of a new block. 
		/// Client code should probably not call this method
		/// </summary>
		public void Flush()
		{
			if (_dataBlock.IsEmpty)
				return;
			if (_closed)
				throw new InvalidOperationException("Cannot add after the table builder was closed");
			if (_pendingIndexEntry)
				throw new InvalidOperationException("Cannot call Flush when pending for an index entry");

			_pendingHandle = WriteBlock(_dataBlock);
			_dataBlock = new BlockBuilder(_dataStream, _storageState, _storageState.InternalKeyComparator, _storageState.Options.BlockRestartInterval);

			_pendingIndexEntry = true;
			_dataStream.Flush();

			if (_filterBuilder != null)
				_filterBuilder.StartBlock(_dataStream.Position);
		}

		public void Finish()
		{
			Flush();
			_closed = true;

			BlockHandle filterBlockHandle = null;

			//write filter block
			if (_filterBuilder != null)
				filterBlockHandle = _filterBuilder.Finish(_dataStream);

			// write metadata block

			var metaIndexBlock = new BlockBuilder(_dataStream, _storageState, _storageState.InternalKeyComparator, _storageState.Options.BlockRestartInterval);
			if (filterBlockHandle != null)
			{
				metaIndexBlock.Add(("filter." + _storageState.Options.FilterPolicy.Name), filterBlockHandle.AsStream());
			}
			var metadIndexBlockHandle = WriteBlock(metaIndexBlock);

			// write index block

			if (_pendingIndexEntry)
			{
				var newKey = _storageState.InternalKeyComparator.FindShortestSuccessor(_lastKey, ref _scratchBuffer);
				_indexBlock.Add(newKey, _pendingHandle.AsStream());
				_pendingIndexEntry = false;
			}

			var indexBlockSize = _indexBlock.Finish();
			_indexBlock.Stream.WriteByte(0);//write type, uncompressed
			_indexBlock.Stream.WriteInt32(Crc.Mask(_indexBlock.Stream.WriteCrc));
			_indexBlock.Stream.Position = _originalIndexStreamPosition;

			var indexBlockHandler = new BlockHandle
				{
					Position = _dataStream.Position,
					Count = indexBlockSize
				};

			_indexBlock.Stream.Stream.CopyTo(_dataStream);

			// write footer
			var footer = new Footer
				{
					IndexHandle = indexBlockHandler,
					MetaIndexHandle = metadIndexBlockHandle
				};

			footer.EncodeTo(_dataStream);
		}

		private BlockHandle WriteBlock(BlockBuilder block)
		{
			// File format contains a sequence of blocks where each block has:
			//    block_data: uint8[n]
			//    type: uint8 - right now always uncompressed
			//    crc: uint32
			var size = block.Finish();

			var originalPosition = block.OriginalPosition;

			var handle = new BlockHandle
				{
					Count = size,
					Position = originalPosition
				};

			// write trailer
			block.Stream.WriteByte(0); // type - uncompressed
			_dataStream.WriteInt32(Crc.Mask(block.Stream.WriteCrc));

			
			block.Dispose();

			return handle;
		}

		public void Dispose()
		{
			_storageState.Options.BufferPool.Return(_lastKeyBuffer);
			_storageState.Options.BufferPool.Return(_scratchBuffer);

			_lastKeyBuffer = null;
			_scratchBuffer = null;

			if(_indexBlock != null)
				_indexBlock.Dispose();
			if(_dataBlock != null)
				_dataBlock.Dispose();
			if (_filterBuilder != null)
				_filterBuilder.Dispose();
			if (_indexStream != null)
				_indexStream.Dispose();
			if (_filterBlockStream != null)
				_filterBlockStream.Dispose();
			if (_dataStream != null)
				_dataStream.Dispose();

			if (_temporaryFiles != null)
				_temporaryFiles.Dispose();
		}
	}
}
