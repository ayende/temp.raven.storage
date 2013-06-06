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

		public void Write(WriteBatch batch)
		{
			WriteAsync(batch).Wait();
		}

		public async Task WriteAsync(WriteBatch batch)
		{
			var mine = new OutstandingWrite(batch);
			this.pendingWrites.Enqueue(mine);

			while (mine.Done() == false && this.pendingWrites.Peek() != mine)
			{
				await this.writeCompletedEvent.WaitAsync();
			}

			if (mine.Done())
				return;

			using (var locker = await this.state.Lock.LockAsync())
			{
				try
				{
					if (mine.Done())
						return;

					await MakeRoomForWrite(force: false, lockScope: locker);

					var lastSequence = this.state.VersionSet.LastSequence;

					var list = BuildBatchGroup(mine);

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
							write.Batch.Prepare(this.state.MemTable);
						}

						await WriteBatch.WriteToLog(list.Select(x => x.Batch).ToArray(), currentSequence, this.state);

						foreach (var write in list)
						{
							write.Batch.Apply(this.state.MemTable, currentSequence);
						}
					}
					await locker.LockAsync();
					this.state.VersionSet.LastSequence = lastSequence;

					foreach (var outstandingWrite in list)
					{
						// notify items we already worked on...
						outstandingWrite.Result.SetResult(null);
					}
				}
				finally
				{
					this.writeCompletedEvent.Pulse();
				}
			}
		}

		private async Task MakeRoomForWrite(bool force, AsyncLock.LockScope lockScope)
		{
			bool allowDelay = force == false;
			while (true)
			{
				if (this.state.BackgroundTask.IsCanceled || this.state.BackgroundTask.IsFaulted)
				{
					await this.state.BackgroundTask;// throws
				}
				else if (allowDelay && this.state.VersionSet.GetNumberOfFilesAtLevel(0) >= Config.SlowdownWritesTrigger)
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
				else if (force == false && this.state.MemTable.ApproximateMemoryUsage <= this.state.Options.WriteBatchSize)
				{
					// There is room in current memtable
					break;
				}
				else if (this.state.ImmutableMemTable != null)
				{
					// We have filled up the current memtable, but the previous
					// one is still being compacted, so we wait.
					await this.state.BackgroundTask;
				}
				else if (this.state.VersionSet.GetNumberOfFilesAtLevel(0) >= Config.StopWritesTrigger)
				{
					// There are too many level-0 files.
					await this.state.BackgroundTask;
				}
				else
				{
					// Attempt to switch to a new memtable and trigger compaction of old
					Debug.Assert(this.state.VersionSet.PrevLogNumber == 0);

					this.state.LogWriter.Dispose();

					this.state.CreateNewLog();
					this.state.ImmutableMemTable = this.state.MemTable;
					this.state.MemTable = new MemTable(this.state);
					force = false;
					await this.state.Compactor.MaybeScheduleCompaction(lockScope);
				}
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

			OutstandingWrite item;
			while (maxSize >= 0 && this.pendingWrites.TryDequeue(out item))
			{
				if(item == mine)
					continue;

				list.Add(item);

				maxSize -= item.Size;
			}

			return list;
		}
	}
}