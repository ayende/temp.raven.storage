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

		public async Task WriteAsync(WriteBatch batch)
		{
			if (Log.IsDebugEnabled)
				Log.Debug(batch.DebugVal);

			var mine = new OutstandingWrite(batch);
			_pendingWrites.Enqueue(mine);

			List<OutstandingWrite> list = null;

			await semaphore.WaitAsync();

			try
			{
				if (mine.Done)
				{
					if (Log.IsDebugEnabled)
						Log.Debug("Write batch #{0} was completed early, done (lock was taken & released).",
								   batch.BatchId);
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

					lastSequence += (ulong)list.Sum(x => x.Batch.OperationCount);

					// Add to log and apply to memtable.  We can release the lock
					// during this phase since mine is currently responsible for logging
					// and protects against concurrent loggers and concurrent writes
					// into the mem table.

					locker.Exit();
					{
						foreach (var write in list)
						{
							write.Batch.Prepare(_state.MemTable);
						}

						await WriteBatch.WriteToLogAsync(list.Select(x => x.Batch).ToArray(), currentSequence, _state).ConfigureAwait(false);

						foreach (var write in list)
						{
							write.Batch.Apply(_state.MemTable, currentSequence);
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
					foreach (OutstandingWrite item in list)
					{
						Debug.Assert(_pendingWrites.Peek() == item);
						OutstandingWrite write;
						_pendingWrites.TryDequeue(out write);
						write.Done = true;
					}
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

				list.Add(item);

				maxSize -= item.Size;
			}

			return list;
		}

		private class OutstandingWrite
		{
			public OutstandingWrite(WriteBatch batch)
			{
				Batch = batch;
				Size = batch.Size;
			}

			public WriteBatch Batch { get; private set; }

			public long Size { get; private set; }

			public bool Done;
		}
	}
}