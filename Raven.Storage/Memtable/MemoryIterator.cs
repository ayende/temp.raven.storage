using System.IO;
using Raven.Storage.Data;
using Raven.Storage.Memory;
using Raven.Storage.Reading;

namespace Raven.Storage.Memtable
{
	public class MemoryIterator : IIterator
	{
		private readonly MemTable _table;
		private readonly SkipList<InternalKey, UnamangedMemoryAccessor.MemoryHandle>.Iterator _iterator;

		public MemoryIterator(MemTable table, SkipList<InternalKey, UnamangedMemoryAccessor.MemoryHandle>.Iterator iterator)
		{
			_table = table;
			_iterator = iterator;
		}

		public void Dispose()
		{
		}

		public bool IsValid { get { return _iterator.IsValid; } }
		public void SeekToFirst()
		{
			_iterator.SeekToFirst();
		}

		public void SeekToLast()
		{
			_iterator.SeekToLast();
		}

		public void Seek(Slice target)
		{
			_iterator.Seek(new InternalKey(target));
		}

		public void Next()
		{
			_iterator.Next();
		}

		public void Prev()
		{
			_iterator.Prev();
		}

		public Slice Key { get { return _iterator.Key.TheInternalKey; } }

		public Stream CreateValueStream()
		{
			return _table.Read(_iterator.Val);
		}
	}
}