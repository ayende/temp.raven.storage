using System.Collections.Generic;
using System.IO;
using Raven.Storage.Data;
using System.Linq;
using Raven.Storage.Memory;
using Raven.Storage.Memtable;

namespace Raven.Storage
{
	public class WriteBatch
	{
		private readonly List<Operation>  _operations = new List<Operation>();
		private enum Operations
		{
			Put,
			Delete
		}

		private class Operation
		{
			public Operations Op;
			public Slice Key;
			public Stream Value;
			public UnamangedMemoryAccessor.MemoryHandle Handle;
		}

		public long Size
		{
			get { return _operations.Sum(x => x.Key.Count + x.Op == Operations.Put ? x.Value.Length : 0); }
		}

		public void Put(Slice key, Stream value)
		{
			_operations.Add(new Operation
				{
					Value = value,
					Key = key,
					Op = Operations.Put
				});
		}

		public void Delete(Slice key)
		{
			_operations.Add(new Operation
				{
					Key = key,
					Op = Operations.Delete
				});
		}

		public void Apply(MemTable memTable, ulong seq)
		{
			foreach (var operation in _operations)
			{
				var itemType = operation.Op == Operations.Delete ? ItemType.Deletion : ItemType.Value;
				memTable.Add(seq ++ , itemType, operation.Key, operation.Handle);
			}
		}

		public void Prepare(MemTable memTable)
		{
			foreach (var operation in _operations)
			{
				operation.Handle = memTable.Write(operation.Value);
			}
		}
	}
}