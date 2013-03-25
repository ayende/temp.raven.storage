using System;
using System.IO;
using System.Text;
using Raven.Storage.Data;
using Raven.Storage.Util;

namespace Raven.Storage.Building
{
    public class TableBuilder
    {
        private readonly StorageOptions _storageOptions;
        private readonly Stream _stream;
        private bool _closed;

        private ArraySegment<byte> _lastKey;
        private readonly IFilterBuilder _filterBuilder;
        private int _numEntries;
        private readonly BlockBuilder _indexBlock;

        private BlockBuilder _dataBlock;

        private bool _pendingIndexEntry;  // if this is true, the data block is empty
        private BlockHandle _pendingHandle;

        public TableBuilder(StorageOptions storageOptions, Stream stream)
        {
            _storageOptions = storageOptions;
            _stream = stream;

            if (storageOptions.FilterPolicy != null)
            {
                _filterBuilder = storageOptions.FilterPolicy.CreateBuidler();
                _filterBuilder.StartBlock(0);
            }

            _indexBlock = new BlockBuilder(new MemoryStream(), storageOptions);
            _dataBlock = new BlockBuilder(stream, storageOptions);
        }

        public int NumEntries
        {
            get { return _numEntries; }
        }

        public long FileSize
        {
            get { return _stream.Position; }
        }

		public void Add(string key, Stream value)
		{
			Add(ToArraySegment(key), value);
		}

        /// <summary>
        /// Add the key and value to the table.
        /// The key's underlying array must not be changed until the _next_ call to add.
        /// The value stream can be disposed after the Add method has been returned.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
        public void Add(ArraySegment<byte> key, Stream value)
        {
            if(_closed)
                throw new InvalidOperationException("Cannot add after the table builder was closed");

            if (_lastKey != null && _storageOptions.Comparator.Compare(key, _lastKey) <= 0)
                throw new InvalidOperationException("Keys must be added in increasing order");

            if (_pendingIndexEntry)
            {
                if (_dataBlock.IsEmpty == false)
                    throw new InvalidOperationException("Cannot have pending index entry with a non empty data block");

                var seperator = _storageOptions.Comparator.FindShortestSeparator(_lastKey, key);

                _indexBlock.Add(seperator, _pendingHandle.AsStream());

                _pendingIndexEntry = false;
            }

            if (_filterBuilder != null)
                _filterBuilder.Add(key);

            _numEntries++;
            _lastKey = key;
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

            _pendingIndexEntry = true;
            _stream.Flush();

            if (_filterBuilder != null)
                _filterBuilder.StartBlock(_stream.Position);
        }

        public void Finish()
        {
            Flush();
            _closed = true;

            BlockHandle filterBlockHandle = null;

            //write filter block
            if (_filterBuilder != null)
                filterBlockHandle = _filterBuilder.Finish(_stream);

            // write metadata block

            var metaIndexBlock = new BlockBuilder(_stream, _storageOptions);
            if (filterBlockHandle != null)
            {
                metaIndexBlock.Add(ToArraySegment("filter." + _storageOptions.FilterPolicy.Name), filterBlockHandle.AsStream());
            }
            var metadIndexBlockHandle = WriteBlock(metaIndexBlock);

            // write index block

            if (_pendingIndexEntry)
            {
                var newKey = _storageOptions.Comparator.FindShortestSuccessor(_lastKey);
                _indexBlock.Add(newKey, _pendingHandle.AsStream());
            }
            var indexBlockHandler = WriteBlock(_indexBlock);

            // write footer
            var footer = new Footer
                {
                    IndexHandle = indexBlockHandler,
                    MetaIndexHandle = metadIndexBlockHandle
                };

            footer.EncodeTo(_stream);
        }

        private ArraySegment<byte> ToArraySegment(string str)
        {
            return new ArraySegment<byte>(Encoding.UTF8.GetBytes(str));
        }

        private BlockHandle WriteBlock(BlockBuilder block)
        {
            // File format contains a sequence of blocks where each block has:
            //    block_data: uint8[n]
            //    type: uint8 - right now always uncompressed
            //    crc: uint32
            var size = block.Finish();

            var handle = new BlockHandle
                {
                    Count = size,
                    Position = block.OriginalPosition
                };

            // write trailer
            _dataBlock.Stream.WriteByte(0); // type - uncompressed
            _stream.Write32BitInt((int)_dataBlock.Stream.WriteCrc);

            _dataBlock = new BlockBuilder(_stream, _storageOptions);

            return handle; 
        }
    }
}