using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Raven.Storage.Data;
using System.Linq;
using Raven.Storage.Impl;
using Raven.Storage.Impl.Streams;
using Raven.Storage.Memory;
using Raven.Storage.Memtable;
using Raven.Storage.Util;

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

		internal void Apply(MemTable memTable, ulong seq)
		{
			foreach (var operation in _operations)
			{
				var itemType = operation.Op == Operations.Delete ? ItemType.Deletion : ItemType.Value;
				memTable.Add(seq ++ , itemType, operation.Key, operation.Handle);
			}
		}

		internal void Prepare(MemTable memTable)
		{
			foreach (var operation in _operations)
			{
				operation.Handle = memTable.Write(operation.Value);
			}
		}

		internal static async Task WriteToLog(WriteBatch[] writes, ulong seq, StorageState state)
		{
			state.LogWriter.RecordStarted();

			var opCount = writes.Sum(x => x._operations.Count);

			var buffer = BitConverter.GetBytes(seq);
			await state.LogWriter.WriteAsync(buffer, 0, buffer.Length);
			buffer = BitConverter.GetBytes(opCount);
			await state.LogWriter.WriteAsync(buffer, 0, buffer.Length);

			foreach (var operation in writes.SelectMany(writeBatch => writeBatch._operations))
			{
				buffer[0] = (byte) operation.Op;
				await state.LogWriter.WriteAsync(buffer, 0, 1);
				await state.LogWriter.Write7BitEncodedIntAsync(operation.Key.Count);
				await state.LogWriter.WriteAsync(operation.Key.Array, operation.Key.Offset, operation.Key.Count);
				if (operation.Op != Operations.Put)
					continue;
				using (var stream = state.MemTable.Read(operation.Handle))
					await state.LogWriter.CopyFromAsync(stream);
			}

			await state.LogWriter.RecordCompletedAsync();
		}
	}
}