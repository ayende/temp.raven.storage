using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Storage.Data;
using System.Linq;
using Raven.Storage.Impl;
using Raven.Storage.Impl.Streams;
using Raven.Storage.Memory;
using Raven.Storage.Memtable;
using Raven.Storage.Reading;
using Raven.Storage.Util;
using Raven.Temp.Logging;

namespace Raven.Storage
{
	public class WriteBatch
	{
	    private static readonly ILog log = LogManager.GetCurrentClassLogger();
	    private static int _counter;

		private readonly List<Operation>  _operations = new List<Operation>();
		private enum Operations
		{
			Put,
			Delete
		}

		public bool DontDisposeStreamsAfterWrite { get; set; }

		private class Operation
		{
			public Operations Op;
			public Slice Key;
			public Stream Value;
			public UnamangedMemoryAccessor.MemoryHandle Handle;
		}

	    public WriteBatch()
	    {
	        BatchId = Interlocked.Increment(ref _counter);
	    }

	    public int BatchId { get; private set; }

	    public long Size
		{
			get { return _operations.Sum(x => x.Key.Count + x.Op == Operations.Put ? x.Value.Length : 0); }
		}

		public int OperationCount
		{
			get { return _operations.Count; }
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
				if (DontDisposeStreamsAfterWrite)
					continue;
				operation.Value.Dispose();
			}
		}

		internal static async Task WriteToLogAsync(WriteBatch[] writes, ulong seq, StorageState state, WriteOptions writeOptions)
		{
            var opCount = writes.Sum(x => x._operations.Count);

            log.Debug("Writing {0} operations in seq {1}", opCount, seq);

            state.LogWriter.RecordStarted(writeOptions.FlushToDisk);

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
				buffer = BitConverter.GetBytes(operation.Handle.Size);
				await state.LogWriter.WriteAsync(buffer, 0, buffer.Length);
				using (var stream = state.MemTable.Read(operation.Handle))
				{
					await state.LogWriter.CopyFromAsync(stream);
				}
			}

			await state.LogWriter.RecordCompletedAsync();

            log.Debug("Wrote {0} operations in seq {1} to log.", opCount, seq);
		}

		internal static IEnumerable<LogReadResult> ReadFromLog(Stream logFile, BufferPool bufferPool)
		{
			var logReader = new LogReader(logFile, true, 0, bufferPool);
			Stream logRecordStream;

			while (logReader.TryReadRecord(out logRecordStream))
			{
				var batch = new WriteBatch();
				ulong seq;
				using (logRecordStream)
				{
					var buffer = new byte[8];
					logRecordStream.ReadExactly(buffer, 8);
					seq = BitConverter.ToUInt64(buffer, 0);
					logRecordStream.ReadExactly(buffer, 4);
					var opCount = BitConverter.ToInt32(buffer, 0);

					for (var i = 0; i < opCount; i++)
					{
						logRecordStream.ReadExactly(buffer, 1);
						var op = (Operations) buffer[0];
						var keyCount = logRecordStream.Read7BitEncodedInt();
						var array = new byte[keyCount];
						logRecordStream.ReadExactly(array, keyCount);

						var key = new Slice(array);

						switch (op)
						{
							case Operations.Delete:
								batch.Delete(key);
								break;
							case Operations.Put:
								logRecordStream.ReadExactly(buffer, 4);
								var size = BitConverter.ToInt64(buffer, 0);
								var value = new MemoryStream();
								logRecordStream.CopyTo(value, size, LogWriter.BlockSize);
								batch.Put(key, value);
								break;
							default:
								throw new ArgumentException("Invalid operation type: " + op);
						}
					}
				}

				yield return new LogReadResult
					{
						WriteSequence = seq,
						WriteBatch = batch
					};
			}
		}

	    public string DebugVal
	    {
	        get { 
                var sb = new StringBuilder("Batch: #").Append(BatchId).Append(" with ").Append(_operations.Count).Append(" operations.").AppendLine();
	            foreach (var operation in _operations)
	            {
	                sb.Append("\t").Append(operation.Op).Append(" ").Append(operation.Key).AppendLine();
	            }
	            return sb.ToString();
	        }
	    }
	}
}