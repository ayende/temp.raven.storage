using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using Raven.Storage.Comparing;
using Raven.Storage.Data;
using Raven.Storage.Memory;
using Raven.Storage.Reading;
using Raven.Storage.Util;

namespace Raven.Storage.Memtable
{
	using Raven.Storage.Impl;

	public class MemTable : IDisposable
	{
		private readonly BufferPool _bufferPool;
		private readonly SkipList<InternalKey, UnamangedMemoryAccessor.MemoryHandle> _table;
		private readonly UnamangedMemoryAccessor _memoryAccessor;
		private readonly InternalKeyComparator _internalKeyComparator;

		public DateTime CreatedAt { get; private set; }

		public MemTable(IStorageContext storageContext)
			: this(storageContext.Options.WriteBatchSize, storageContext.InternalKeyComparator, storageContext.Options.BufferPool)
		{

		}

		public MemTable(int writeBatchSize, InternalKeyComparator internalKeyComparator, BufferPool bufferPool)
		{
			CreatedAt = DateTime.UtcNow;
			_bufferPool = bufferPool;
			_memoryAccessor = new UnamangedMemoryAccessor(writeBatchSize);

			_internalKeyComparator = internalKeyComparator;
			_table = new SkipList<InternalKey, UnamangedMemoryAccessor.MemoryHandle>(_internalKeyComparator);
		}

		public int ApproximateMemoryUsage { get; private set; }

		public int Count { get { return _table.Count; } }

		public UnamangedMemoryAccessor.MemoryHandle Write(Stream value)
		{
			if (value == null)
				return null;
			var memoryHandle = _memoryAccessor.Write(value);
			ApproximateMemoryUsage += memoryHandle.Size;
			return memoryHandle;
		}

		public IIterator NewIterator()
		{
			return new MemoryIterator(this, _table.NewIterator());
		}

		public IEnumerable<Slice> AllKeys
		{
			get
			{
				using (var it = NewIterator())
				{
					it.SeekToFirst();
					while (it.IsValid)
					{
						yield return it.Key;
						it.Next();
					}
				}
			}
		}

		public void Add(ulong seq, ItemType type, Slice key, UnamangedMemoryAccessor.MemoryHandle memoryHandle)
		{
			var internalKey = new InternalKey(key, seq, type);

			_table.Insert(internalKey, memoryHandle);
		}

		/// <summary>
		/// Returns if the value is found in this mem table or not.
		/// Note that it is posible for the value to be found and the stream to be null, if the value
		/// has been deleted
		/// </summary>
		public bool TryGet(Slice userKey, ulong sequence, out Stream stream)
		{
			var memKey = new InternalKey(userKey, sequence, ItemType.ValueForSeek);
			var iterator = _table.NewIterator();
			iterator.Seek(memKey);
			if (iterator.IsValid == false ||
				_internalKeyComparator.Compare(memKey, iterator.Key) > 0)
			{
				stream = null;
				return false;
			}

			switch (iterator.Key.Type)
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


		public void Dispose()
		{
			_memoryAccessor.Dispose();
		}

		public Stream Read(UnamangedMemoryAccessor.MemoryHandle handle)
		{
			return _memoryAccessor.Read(handle);
		}
	}
}