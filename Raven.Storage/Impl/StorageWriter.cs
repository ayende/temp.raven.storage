using System.Threading;

namespace Raven.Storage.Impl
{
	using Raven.Storage.Memtable;

	using System;
	using System.Collections.Concurrent;
	using System.Collections.Generic;
	using System.Diagnostics;
	using System.Linq;
	using System.Threading.Tasks;

	public class StorageWriter
	{
		private readonly StorageState state;
		private readonly ConcurrentQueue<OutstandingWrite> pendingWrites = new ConcurrentQueue<OutstandingWrite>();
		private readonly AsyncMonitor writeCompletedEvent = new AsyncMonitor();

		private class OutstandingWrite
		{
			public WriteBatch Batch { get; private set; }
			public TaskCompletionSource<object> Result { get; private set; }

			public OutstandingWrite(WriteBatch batch)
			{
				Batch = batch;
				Size = batch.Size;
				Result = new TaskCompletionSource<object>();
			}

			public long Size { get; private set; }

			public bool Done()
			{
				var task = Result.Task;
				if (task.IsCompleted)
					return true;
				if (task.IsCanceled || task.IsFaulted)
					task.Wait(); // throws
				return false;
			}
		}

		public StorageWriter(StorageState state)
		{
			this.state = state;
		}

		public async Task WriteAsync(WriteBatch batch)
		{
			var mine = new OutstandingWrite(batch);
			pendingWrites.Enqueue(mine);

			while (mine.Done() == false && pendingWrites.Peek() != mine)
			{
				await writeCompletedEvent.WaitAsync();
			}

			if (mine.Done())
				return;

			using (var locker = await state.Lock.LockAsync())
			{
				List<OutstandingWrite> list = null;
				try
				{
					if (mine.Done())
						return;

					await MakeRoomForWriteAsync(force: false, lockScope: locker);

					var lastSequence = state.VersionSet.LastSequence;

					list = BuildBatchGroup(mine);

					var currentSequence = lastSequence + 1;

					lastSequence += (ulong)list.Count;

					// Add to log and apply to memtable.  We can release the lock
					// during this phase since mine is currently responsible for logging
					// and protects against concurrent loggers and concurrent writes
					// into the mem table.

					locker.Exit();
					{
						foreach (var write in list)
						{
							write.Batch.Prepare(state.MemTable);
						}

						await WriteBatch.WriteToLogAsync(list.Select(x => x.Batch).ToArray(), currentSequence, state);


						foreach (var write in list)
						{
							write.Batch.Apply(state.MemTable, currentSequence);
						}
					}
					await locker.LockAsync();
					state.VersionSet.LastSequence = lastSequence;

					foreach (var outstandingWrite in list)
					{
						// notify items we already worked on...
						outstandingWrite.Result.SetResult(null);
					}
				}
				finally
				{
					if (list != null)
					{
						foreach (var item in list)
						{
							Debug.Assert(pendingWrites.Peek() == item);
							OutstandingWrite _;
							pendingWrites.TryDequeue(out _);
						}
					}
					writeCompletedEvent.Pulse();
				}
			}
		}

		private async Task MakeRoomForWriteAsync(bool force, AsyncLock.LockScope lockScope)
		{
			bool allowDelay = force == false;
			while (true)
			{
				await lockScope.LockAsync();
				if (state.BackgroundTask.IsCanceled || state.BackgroundTask.IsFaulted)
				{
					await state.BackgroundTask;// throws
				}
				else if (allowDelay && state.VersionSet.GetNumberOfFilesAtLevel(0) >= Config.SlowdownWritesTrigger)
				{
					// We are getting close to hitting a hard limit on the number of
					// L0 files.  Rather than delaying a single write by several
					// seconds when we hit the hard limit, start delaying each
					// individual write by 1ms to reduce latency variance.  Also,
					// this delay hands over some CPU to the compaction thread in
					// case it is sharing the same core as the writer.
					lockScope.Exit();
					{
						await Task.Delay(TimeSpan.FromMilliseconds(1));
					}
					await lockScope.LockAsync();
					allowDelay = false; // Do not delay a single write more than once
				}
				else if (force == false && state.MemTable.ApproximateMemoryUsage <= state.Options.WriteBatchSize)
				{
					// There is room in current memtable
					break;
				}
				else if (state.ImmutableMemTable != null)
				{
					// We have filled up the current memtable, but the previous
					// one is still being compacted, so we wait.
					await state.BackgroundTask;
				}
				else if (state.VersionSet.GetNumberOfFilesAtLevel(0) >= Config.StopWritesTrigger)
				{
					// There are too many level-0 files.
					await state.BackgroundTask;
				}
				else
				{
					// Attempt to switch to a new memtable and trigger compaction of old
					Debug.Assert(state.VersionSet.PrevLogNumber == 0);

					state.LogWriter.Dispose();

					state.CreateNewLog();
					state.ImmutableMemTable = state.MemTable;
					state.MemTable = new MemTable(state);
					force = false;
					state.Compactor.MaybeScheduleCompaction(lockScope);
				}

				lockScope.Exit();
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

			foreach (var item in pendingWrites)
			{
				if (maxSize <= 0)
					break;

				if (item == mine)
					continue;
				maxSize -= item.Size;
			}

			return list;
		}
	}
}