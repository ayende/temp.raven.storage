using System;
using System.IO;
using System.Text;
using Raven.Storage.Data;
using Raven.Storage.Filtering;
using Raven.Storage.Util;

namespace Raven.Storage.Building
{
	public class TableBuilder : IDisposable
	{
		private byte[] _scratchBuffer;
		private readonly byte[] _lastKeyBuffer;
		private readonly StorageOptions _storageOptions;
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
		public TableBuilder(StorageOptions storageOptions,
			Stream dataStream,
			Func<Stream> tempStreamGenerator)
		{
			try
			{
				_storageOptions = storageOptions;
				_dataStream = dataStream;
				_indexStream = tempStreamGenerator();
				_originalIndexStreamPosition = _indexStream.Position;

				_lastKeyBuffer = new byte[_storageOptions.MaximumExpectedKeySize];
				_scratchBuffer = new byte[_storageOptions.MaximumExpectedKeySize];


				if (storageOptions.FilterPolicy != null)
				{
					var filterBuilder = storageOptions.FilterPolicy.CreateBuilder();
					_filterBlockStream = tempStreamGenerator();
					_filterBuilder = new FilterBlockBuilder(_filterBlockStream, filterBuilder);
					_filterBuilder.StartBlock(0);
				}

				_indexBlock = new BlockBuilder(_indexStream, storageOptions);
				_dataBlock = new BlockBuilder(_dataStream, storageOptions);
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

			if (_storageOptions.Comparator.Compare(key, _lastKey) <= 0)
				throw new InvalidOperationException("Keys must be added in increasing order");

			if (_pendingIndexEntry)
			{
				if (_dataBlock.IsEmpty == false)
					throw new InvalidOperationException("Cannot have pending index entry with a non empty data block");

				var seperator = _storageOptions.Comparator.FindShortestSeparator(_lastKey, key, ref _scratchBuffer);

				_indexBlock.Add(seperator, _pendingHandle.AsStream());

				_pendingIndexEntry = false;
			}

			if (_filterBuilder != null)
				_filterBuilder.Add(key);

			_numEntries++;
			_lastKey = new Slice(_lastKeyBuffer, 0, key.Count);
			Buffer.BlockCopy(key.Array, key.Offset, _lastKeyBuffer, 0, key.Count);
			_dataBlock.Add(key, value);

			if (_dataBlock.EstimatedSize >= _storageOptions.BlockSize)
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
			_dataBlock = new BlockBuilder(_dataStream, _storageOptions);

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

			var metaIndexBlock = new BlockBuilder(_dataStream, _storageOptions);
			if (filterBlockHandle != null)
			{
				metaIndexBlock.Add(("filter." + _storageOptions.FilterPolicy.Name), filterBlockHandle.AsStream());
			}
			var metadIndexBlockHandle = WriteBlock(metaIndexBlock);

			// write index block

			if (_pendingIndexEntry)
			{
				var newKey = _storageOptions.Comparator.FindShortestSuccessor(_lastKey, ref _scratchBuffer);
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

			_indexBlock.Stream.CopyTo(_dataStream);

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

			return handle;
		}

		public void Dispose()
		{
			if (_indexStream != null)
				_indexStream.Dispose();
			if (_filterBlockStream != null)
				_filterBlockStream.Dispose();
		}
	}
}