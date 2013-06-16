using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Raven.Abstractions.Logging;
using Raven.Storage.Memtable;

namespace Raven.Storage.Impl
{
    public class StorageWriter
    {
        private static readonly ILog Log = LogManager.GetCurrentClassLogger();
        private readonly ConcurrentQueue<OutstandingWrite> _pendingWrites = new ConcurrentQueue<OutstandingWrite>();
        private readonly StorageState _state;
        private readonly AsyncEvent _writeCompletedEvent = new AsyncEvent();

        public StorageWriter(StorageState state)
        {
            _state = state;
        }

        public async Task WriteAsync(WriteBatch batch)
        {
            Log.Debug("Enqueued write batch #{0}.", batch.BatchId);
            var mine = new OutstandingWrite(batch);
            _pendingWrites.Enqueue(mine);

            var state = new Reference<int>();
            while (mine.Done() == false && _pendingWrites.Peek() != mine)
            {
                Log.Debug("Not the only concurrent write for write batch # {0}, waiting...", batch.BatchId);
                await _writeCompletedEvent.WaitAsync(state);
            }

            if (mine.Done())
            {
                Log.Debug("Write batch #{0} was completed early, done (no lock needed), will pulse.", batch.BatchId);
                _writeCompletedEvent.PulseAll();
                return;
            }

            List<OutstandingWrite> list = null;
            try
            {
                using (AsyncLock.LockScope locker = await _state.Lock.LockAsync())
                {
                    if (mine.Done())
                    {
                        Log.Debug("Write batch #{0} was completed early, done (lock was taken & released).",
                                   batch.BatchId);
                        return;
                    }

                    await MakeRoomForWriteAsync(force: false, lockScope: locker);

                    ulong lastSequence = _state.VersionSet.LastSequence;

                    list = BuildBatchGroup(mine);

                    if (list.Count > 1)
                    {
                        Log.Debug("Write batch #{0} will be written along with {1} batches, all at once.",
                                   batch.BatchId, list.Count);
                    }

                    ulong currentSequence = lastSequence + 1;

                    lastSequence += (ulong) list.Count;

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

                        await WriteBatch.WriteToLogAsync(list.Select(x => x.Batch).ToArray(), currentSequence, _state);


                        foreach (var write in list)
                        {
                            write.Batch.Apply(_state.MemTable, currentSequence);
                        }
                    }
                    await locker.LockAsync();
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
                        write.Result.SetResult(null);
                    }
                }
                Log.Debug("Pulsing all pending writes from batch #{0}", batch.BatchId);
                _writeCompletedEvent.PulseAll();
            }
        }

        private async Task MakeRoomForWriteAsync(bool force, AsyncLock.LockScope lockScope)
        {
            bool allowDelay = force == false;
            while (true)
            {
                await lockScope.LockAsync();
                if (_state.BackgroundTask.IsCanceled || _state.BackgroundTask.IsFaulted)
                {
                    await _state.BackgroundTask; // throws
                }
                else if (allowDelay && _state.VersionSet.GetNumberOfFilesAtLevel(0) >= Config.SlowdownWritesTrigger)
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
                else if (force == false && _state.MemTable.ApproximateMemoryUsage <= _state.Options.WriteBatchSize)
                {
                    // There is room in current memtable
                    break;
                }
                else if (_state.ImmutableMemTable != null)
                {
                    // We have filled up the current memtable, but the previous
                    // one is still being compacted, so we wait.
                    await _state.BackgroundTask;
                }
                else if (_state.VersionSet.GetNumberOfFilesAtLevel(0) >= Config.StopWritesTrigger)
                {
                    // There are too many level-0 files.
                    await _state.BackgroundTask;
                }
                else
                {
                    // Attempt to switch to a new memtable and trigger compaction of old
                    Debug.Assert(_state.VersionSet.PrevLogNumber == 0);

                    _state.LogWriter.Dispose();

                    _state.CreateNewLog();
                    _state.ImmutableMemTable = _state.MemTable;
                    _state.MemTable = new MemTable(_state);
                    force = false;
                    _state.Compactor.MaybeScheduleCompaction(lockScope);
                }

                lockScope.Exit();
            }
        }


        private List<OutstandingWrite> BuildBatchGroup(OutstandingWrite mine)
        {
            // Allow the group to grow up to a maximum size, but if the
            // original write is small, limit the growth so we do not slow
            // down the small write too much.
            long maxSize = 1024*1024; // 1 MB by default
            if (mine.Size < 128*1024)
                maxSize = mine.Size + (128*1024);

            var list = new List<OutstandingWrite> {mine};

            foreach (OutstandingWrite item in _pendingWrites)
            {
                if (maxSize <= 0)
                    break;

                if (item == mine)
                    continue;
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
                Result = new TaskCompletionSource<object>();
            }

            public WriteBatch Batch { get; private set; }
            public TaskCompletionSource<object> Result { get; private set; }

            public long Size { get; private set; }

            public bool Done()
            {
                Task<object> task = Result.Task;
                if (task.IsCompleted)
                    return true;
                if (task.IsCanceled || task.IsFaulted)
                    task.Wait(); // throws
                return false;
            }
        }
    }
}