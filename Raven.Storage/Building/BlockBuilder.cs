using System;
using System.Collections.Generic;
using System.IO;
using Raven.Storage.Comparing;
using Raven.Storage.Data;
using Raven.Storage.Impl;
using Raven.Storage.Util;

namespace Raven.Storage.Building
{
    /// <summary>
    /// BlockBuilder generates blocks where keys are prefix-compressed:
    ///
    /// When we store a key, we drop the prefix shared with the previous
    /// string.  This helps reduce the space requirement significantly.
    /// Furthermore, once every K keys, we do not apply the prefix
    /// compression and store the entire key.  We call this a "restart
    /// point".  The tail end of the block stores the offsets of all of the
    /// restart points, and can be used to do a binary search when looking
    /// for a particular key.  Values are stored as-is (without compression)
    /// immediately following the corresponding key.
    ///
    /// An entry for a particular key-value pair has the form:
    ///     shared_bytes: varint32
    ///     unshared_bytes: varint32
    ///     value_length: varint32
    ///     key_delta: char[unshared_bytes]
    ///     value: char[value_length]
    /// shared_bytes == 0 for restart points.
    ///
    /// The trailer of the block has the form:
    ///     restarts: uint32[num_restarts]
    ///     num_restarts: uint32
    /// restarts[i] contains the offset within the block of the ith restart point.
    /// </summary>
    public class BlockBuilder
    {
	    private readonly StorageState _storageState;
	    private readonly CrcStream _stream;
        readonly List<int> _restarts = new List<int> { 0 };// first restart at offset 0
        private int _counter;
        private Slice _lastKey;
	    private byte[] _lastKeyBuffer;
        private int _size;
        private bool _finished;
	    private readonly IComparator _comparator;

		public BlockBuilder(Stream stream, StorageState storageState, IComparator comparator)
        {
	        _storageState = storageState;
			if (_storageState.Options.BlockRestartInterval < 1)
                throw new InvalidOperationException("BlockRestartInternal must be >= 1");
            _stream = new CrcStream(stream);
            IsEmpty = true;
            OriginalPosition = stream.Position;
	        _comparator = comparator;
			_lastKeyBuffer = new byte[storageState.Options.MaximumExpectedKeySize];
        }

        public long OriginalPosition { get; private set; }

        public CrcStream Stream
        {
            get { return _stream; }
        }

        public int EstimatedSize
        {
            get { return _size + ((_restarts.Count + 1) * 4); }
        }

        public bool IsEmpty { get; private set; }

        public void Add(Slice key, Stream value)
        {
            if (_finished)
                throw new InvalidOperationException("Cannot add to a block after it has been finished");
            if (_size > 0 && (_comparator.Compare(key, _lastKey) <= 0))
                throw new InvalidOperationException("Add must be call on items in sorted order");

            var valLen = value.Length - value.Position;
            if (valLen > int.MaxValue)
                throw new InvalidOperationException("Cannot store values that are greater than 2GB");

            IsEmpty = false;

            int shared = 0;
			if (_counter < _storageState.Options.BlockRestartInterval)
            {
                // let us see how much we can share with the prev string
				// intentionally using the user key comparator and not the internal key comparator
				shared = _storageState.Options.Comparator.FindSharedPrefix(_lastKey, key);
            }
            else
            {
                // restart compression
                _restarts.Add(_size);
                _counter = 0;
            }
            int nonShared = key.Count - shared;
            // Add "<shared><non_shared><value_size>"
            _size += _stream.Write7BitEncodedInt(shared);
            _size += _stream.Write7BitEncodedInt(nonShared);
            _size += _stream.Write7BitEncodedInt((int)valLen);
            _stream.Write(key.Array, key.Offset + shared, key.Count - shared);
            _size += key.Count - shared;
            value.CopyTo(_stream);
            _size += (int)valLen;

	        _lastKey = new Slice(ref _lastKeyBuffer, key);

            _counter++;
        }

        public int Finish()
        {
            foreach (var restart in _restarts)
            {
                _stream.WriteInt32(restart);
            }
            _stream.WriteInt32(_restarts.Count);
            _size += (_restarts.Count + 1) * 4;
            _finished = true;

            return (int)(_stream.Position - OriginalPosition);
        }
    }
}