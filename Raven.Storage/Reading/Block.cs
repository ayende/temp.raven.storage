using System;
using System.Collections.Generic;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Threading;
using Raven.Storage.Comparing;
using Raven.Storage.Data;
using Raven.Storage.Exceptions;
using Raven.Storage.Memory;
using Raven.Storage.Util;

namespace Raven.Storage.Reading
{
	public class Block : IDisposable
	{
		public const int BlockTrailerSize = 5; // tag + crc32

		private readonly BlockHandle _handle;
		private readonly StorageOptions _storageOptions;
		private readonly FileData _fileData;
		private readonly IArrayAccessor _accessor;
	
		private int usage;

		public void IncrementUsage()
		{
			Interlocked.Increment(ref usage);
		}

		public Block(StorageOptions storageOptions, ReadOptions readOptions, BlockHandle handle, FileData fileData)
		{
			try
			{
				_handle = handle;
				_storageOptions = storageOptions;
				_fileData = fileData;
				if (handle.Position > fileData.Size || (handle.Position + handle.Count + BlockTrailerSize) > fileData.Size)
					throw new CorruptedDataException("The specified accessor is beyond the bounds of the provided mappedFile");

				_accessor = _fileData.File.CreateAccessor(handle.Position, handle.Count + BlockTrailerSize);

				if (readOptions.VerifyChecksums)
				{
					var crc = Crc.Unmask(_accessor.ReadInt32(handle.Count + 1));
					var actualCrc = CalculateActualCrc(handle.Count + 1); // data + tag
					if (crc != actualCrc)
						throw new CorruptedDataException("block checksum mismatch");
				}
				RestartsCount = _accessor.ReadInt32(handle.Count - sizeof(int));
				RestartsOffset = handle.Count - (RestartsCount * sizeof(int)) - sizeof(int);
				if (RestartsOffset > handle.Count)
					throw new CorruptedDataException("restart offset wrapped around");
			}
			catch (Exception)
			{
				Dispose();
				throw;
			}
		}

		private uint CalculateActualCrc(long crcRange)
		{
			uint actual = unchecked(0xFFFFFFFF);
			for (int i = 0; i < crcRange; i++)
			{
				actual = Crc.CalculateCrc(actual, _accessor[i]);
			}
			return actual ^ 0xFFFFFFFF;
		}

		public long RestartsOffset { get; private set; }

		public int RestartsCount { get; private set; }


		public void Dispose()
		{
			if (Interlocked.Decrement(ref usage) > 0)
				return;

			if (_accessor != null)
				_accessor.Dispose();
		}

		public IIterator CreateIterator(IComparator comparator)
		{
			if (RestartsCount == 0)
				return new EmptyIterator();
			IncrementUsage(); // make sure that this object won't be disposed before its iterator
			return new BlockIterator(comparator, this);
		}

		public class BlockIterator : IIterator
		{
			private readonly IComparator _comparator;
			private readonly Block _parent;
			private int _restartIndex;
			private int _offset, _size;
			private byte[] _keyBuffer;
			private int _currentRecordStart;

			public BlockIterator(IComparator comparator, Block parent)
			{
				_comparator = comparator;
				_parent = parent;
				_keyBuffer = new byte[_parent._storageOptions.MaximumExpectedKeySize];
			}

			private void SeekToRestartPoint(int index)
			{
				_restartIndex = index;
				_offset = GetRestartPoint(index);
				_size = 0;
			}

			private int GetRestartPoint(int index)
			{
				if (index >= _parent.RestartsCount)
					throw new IndexOutOfRangeException();

				return _parent._accessor.ReadInt32(_parent.RestartsOffset + index * sizeof(int));
			}

			public bool ParseNextKey()
			{
				_offset += _size; // skipping the current record
				if (_offset >= _parent.RestartsOffset)
				{
					// no more entries to return
					IsValid = false;
					return false;
				}

				_offset = DecodeNextEntry(_offset);

				while (_restartIndex + 1 < _parent.RestartsCount &&
						GetRestartPoint(_restartIndex + 1) < _offset)
				{
					++_restartIndex;
				}
				IsValid = true;
				return true;
			}

			private int DecodeNextEntry(int currentOffset)
			{
				_currentRecordStart = _offset;
				var shared = _parent._accessor.Read7BitEncodedInt(ref currentOffset);
				var nonShared = _parent._accessor.Read7BitEncodedInt(ref currentOffset);
				_size = _parent._accessor.Read7BitEncodedInt(ref currentOffset);

				EnsureKeyBufferSize(shared, nonShared);

				var read = _parent._accessor.ReadArray(currentOffset, _keyBuffer, shared, nonShared);
				if (read != nonShared)
					throw new CorruptedDataException("Could not read all key non shared data");

				Key = new Slice(_keyBuffer, 0, shared + nonShared);
				return currentOffset + read;
			}

			// trying to make this method small enough so it would be inlined
			// for the happy case of not having to grow the buffer
			private void EnsureKeyBufferSize(int shared, int nonShared)
			{
				var keyBuffer = _keyBuffer;
				if (shared + nonShared <= keyBuffer.Length)
					return;

				CreateNewBuffer(shared, nonShared);
			}

			private void CreateNewBuffer(int shared, int nonShared)
			{
				int size = Info.GetPowerOfTwo(shared + nonShared);
				byte[] keyBuffer = new byte[size];
				Buffer.BlockCopy(_keyBuffer, 0, keyBuffer, 0, shared);
				_keyBuffer = keyBuffer;
			}

			public bool IsValid { get; private set; }

			public void SeekToFirst()
			{
				SeekToRestartPoint(0);
				ParseNextKey();
			}

			public void SeekToLast()
			{
				SeekToRestartPoint(_parent.RestartsCount - 1);
				while (ParseNextKey() && NextEntryOffset() < _parent.RestartsOffset)
				{
					//keep skipping
				}
			}

			private int NextEntryOffset()
			{
				return _offset + _size;
			}

			private void AssertValid()
			{
				if (IsValid == false)
					throw new InvalidOperationException("Cannot call this method when the state of the iterator is not valid");
			}

			public void Seek(Slice target)
			{
				// Binary search in restart array to find the last restart point 
				// with  a key < target

				var left = 0;
				var right = _parent.RestartsCount - 1;
				while (left < right)
				{
					var mid = (left + right + 1) / 2;
					DecodeNextEntry(GetRestartPoint(mid));
					if (_comparator.Compare(Key, target) < 0)
					{
						// key at mid is smaller than target, therefor all blocks before mid are uninteresting
						left = mid;
					}
					{
						// key at mid is >= target, therefor all blocks at or after mid are uninteresting
						right = mid - 1;
					}
				}

				// Linear search (within a restart block) for first key >= target
				SeekToRestartPoint(left);
				while (true)
				{
					if (!ParseNextKey())
						return;
					if (_comparator.Compare(Key, target) >= 0)
						return;
				}
			}

			public void Next()
			{
				AssertValid();
				ParseNextKey();
			}

			public void Prev()
			{
				AssertValid();
				// scan backward to a restart point before the current one.
				int original = _currentRecordStart;
				while (GetRestartPoint(_restartIndex) >= original)
				{
					if (_restartIndex == 0)
					{
						IsValid = false;
						return;
					}
					_restartIndex--;
				}

				SeekToRestartPoint(_restartIndex);
				do
				{
					// Loop until we hit the entry before the one we were just on
				} while (ParseNextKey() && NextEntryOffset() < original);
			}

			public Slice Key { get; private set; }

			public Stream CreateValueStream()
			{
				AssertValid();

				if (_offset + _size > _parent._handle.Count)
					throw new CorruptedDataException("Attempted to read beyond the boundaries of the current block");

				return _parent._fileData.File.CreateStream( _parent._handle.Position + _offset, _size);
			}

			public void Dispose()
			{
				_parent.Dispose();
			}
		}
	}
}