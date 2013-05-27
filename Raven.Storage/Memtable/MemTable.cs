using System;
using System.IO;
using Raven.Storage.Comparing;
using Raven.Storage.Data;
using Raven.Storage.Memory;
using Raven.Storage.Util;

namespace Raven.Storage.Memtable
{
	public class MemTable : IDisposable
	{
		private readonly StorageOptions _storageOptions;
		private readonly SkipList<Slice, UnamangedMemoryAccessor.MemoryHandle> _table;
		private readonly UnamangedMemoryAccessor _memoryAccessor;
		private readonly InternalKeyComparator _internalKeyComparator;

		public MemTable(StorageOptions storageOptions)
		{
			_memoryAccessor = new UnamangedMemoryAccessor(storageOptions.WriteBatchSize);

			_storageOptions = storageOptions;

			_internalKeyComparator = new InternalKeyComparator(_storageOptions.Comparator);
			_table = new SkipList<Slice, UnamangedMemoryAccessor.MemoryHandle>(_internalKeyComparator.Compare);
		}

		public int ApproximateMemoryUsage { get; private set; }

		public UnamangedMemoryAccessor.MemoryHandle Write(Stream value)
		{
			if (value == null)
				return null;
			var memoryHandle = _memoryAccessor.Write(value);
			ApproximateMemoryUsage += memoryHandle.Size;
			return memoryHandle;
		}

		public void Add(ulong seq, ItemType type, Slice key, UnamangedMemoryAccessor.MemoryHandle memoryHandle)
		{
			var buffer = new byte[key.Count + 8];
			Buffer.BlockCopy(key.Array, key.Offset, buffer, 0, key.Count);
			buffer.WriteLong(key.Count, Format.PackSequenceAndType(seq, type));

			var internalKey = new Slice(buffer);

			_table.Insert(internalKey, memoryHandle);
		}

		/// <summary>
		/// Returns if the value is found in this mem table or not.
		/// Note that it is posible for the value to be found and the stream to be null, if the value
		/// has been deleted
		/// </summary>
		public bool TryGet(Slice userKey, ulong sequence, out Stream stream)
		{
			var buffer = _storageOptions.BufferPool.Take(userKey.Count + 8);
			try
			{
				Buffer.BlockCopy(userKey.Array, userKey.Offset, buffer, 0, userKey.Count);
				buffer.WriteLong(userKey.Count, Format.PackSequenceAndType(sequence, ItemType.ValueForSeek));

				var memKey = new Slice(buffer, 0, userKey.Count + 8);
				var iterator = _table.NewIterator();
				iterator.Seek(memKey);
				if (iterator.IsValid == false ||
					_internalKeyComparator.EqualKeys(memKey, iterator.Key) == false)
				{
					stream = null;
					return false;
				}

				var tag = iterator.Key.Array.ReadLong(iterator.Key.Count - 8);
				switch ((ItemType)tag)
				{
					case ItemType.Deletion:
						stream = null;
						return true;
					case ItemType.Value:
						stream = _memoryAccessor.Read(iterator.Val);
						return true;
					default:
						throw new ArgumentOutOfRangeException();
				}
			}
			finally
			{
				_storageOptions.BufferPool.Return(buffer);
			}
		}


		public void Dispose()
		{
			_memoryAccessor.Dispose();
		}

		public Stream Read(UnamangedMemoryAccessor.MemoryHandle handle)
		{
			return _memoryAccessor.Read(handle);
		}

		public SkipList<Slice, UnamangedMemoryAccessor.MemoryHandle>.Iterator NewIterator()
		{
			return _table.NewIterator();
		}
	}
}