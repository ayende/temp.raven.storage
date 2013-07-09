using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Temp.Logging;

namespace Raven.Storage.Impl
{
	using Raven.Storage.Data;
	using Raven.Storage.Exceptions;

	public class StorageWriter
	{
		private static readonly ILog Log = LogManager.GetCurrentClassLogger();
		private readonly ConcurrentQueue<OutstandingWrite> _pendingWrites = new ConcurrentQueue<OutstandingWrite>();
		private readonly StorageState _state;

		public StorageWriter(StorageState state)
		{
			_state = state;
		}

		public SemaphoreSlim semaphore = new SemaphoreSlim(1, 1);

		public async Task WriteAsync(WriteBatch batch, WriteOptions options = null)
		{
			if (Log.IsDebugEnabled)
				Log.Debug(batch.DebugVal);

			if (options == null)
				options = new WriteOptions();

			var mine = new OutstandingWrite(batch, options);
			_pendingWrites.Enqueue(mine);

			List<OutstandingWrite> list = null;

			await semaphore.WaitAsync();

			try
			{
				if (mine.Done)
				{
					if (Log.IsDebugEnabled)
						Log.Debug("Write batch #{0} was completed early, done (no lock needed).", batch.BatchId);
					return;
				}

				using (AsyncLock.LockScope locker = await _state.Lock.LockAsync().ConfigureAwait(false))
				{
					await _state.MakeRoomForWriteAsync(force: false, lockScope: locker).ConfigureAwait(false);

					ulong lastSequence = _state.VersionSet.LastSequence;

					list = BuildBatchGroup(mine);

					if (list.Count > 1)
					{
						if (Log.IsDebugEnabled)
							Log.Debug("Write batch #{0} will be written along with {1} batches, all at once.",
									   batch.BatchId, list.Count);
					}

					ulong currentSequence = lastSequence + 1;
					var currentSequenceRef = new Reference<ulong> { Value = lastSequence + 1 };

					lastSequence += (ulong)list.Sum(x => x.Batch.OperationCount);

					// Add to log and apply to memtable.  We can release the lock
					// during this phase since mine is currently responsible for logging
					// and protects against concurrent loggers and concurrent writes
					// into the mem table.

					locker.Exit();
					{
						list.ForEach(write => write.Batch.Prepare(_state.MemTable));

						try
						{
							await
								Task.WhenAll(
									WriteBatch.WriteToLogAsync(list.Select(x => x.Batch).ToArray(), currentSequence, _state, options),
									Task.Run(() => list.ForEach(write => write.Batch.Apply(_state.MemTable, currentSequenceRef))));
						}
						catch (LogWriterException e)
						{
							Log.ErrorException("Writing to log failed.", e);
							
							currentSequenceRef = new Reference<ulong> { Value = lastSequence + 1 };
							list.ForEach(write => write.Batch.Remove(_state.MemTable, currentSequenceRef));

							throw;
						}
					}

					await locker.LockAsync().ConfigureAwait(false);
					_state.VersionSet.LastSequence = lastSequence;
				}
			}
			finally
			{
				if (list != null)
				{
					int count = 0;
					long size = 0;
					foreach (OutstandingWrite item in list)
					{
						Debug.Assert(_pendingWrites.Peek() == item);
						OutstandingWrite write;
						_pendingWrites.TryDequeue(out write);
						count += write.Batch.OperationCount;
						size += write.Size;
						write.Done = true;
					}
					_state.PerfCounters.Write(count);
					_state.PerfCounters.BytesWritten(size);
				}

				semaphore.Release();
			}
		}

		private List<OutstandingWrite> BuildBatchGroup(OutstandingWrite mine)
		{
			// Allow the group to grow up to a maximum size, but if the
			// original write is small, limit the growth so we do not slow
			// down the small write too much.
			long maxSize = 1024 * 1024; // 1 MB by default
			if (mine.Size < 128 * 1024)
				maxSize = mine.Size + (128 * 1024);

			var list = new List<OutstandingWrite> { mine };

			foreach (OutstandingWrite item in _pendingWrites)
			{
				if (maxSize <= 0)
					break;

				if (item == mine)
					continue;

				if (item.Options.FlushToDisk != mine.Options.FlushToDisk)
					break; // we can only take items that have the same flush to disk behavior

				list.Add(item);

				maxSize -= item.Size;
			}

			return list;
		}

		private class OutstandingWrite
		{
			public OutstandingWrite(WriteBatch batch, WriteOptions options)
			{
				Batch = batch;
				Options = options;
				Size = batch.Size;
			}

			public WriteBatch Batch { get; private set; }
			public WriteOptions Options { get; private set; }

			public long Size { get; private set; }

			public bool Done;
		}
	}
}