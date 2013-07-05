using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;
using System.Threading.Tasks;
using Raven.Temp.Logging;

namespace Raven.Storage.Impl
{
	using Raven.Storage.Data;

	public class StorageWriter
	{
		private static readonly ILog Log = LogManager.GetCurrentClassLogger();
		private readonly ConcurrentQueue<OutstandingWrite> _pendingWrites =
new ConcurrentQueue<OutstandingWrite>();
		private readonly StorageState _state;
		private readonly AsyncEvent _writeCompletedEvent = new AsyncEvent();

		public static Func<Task<bool>> CreateTask = CreateTaskFactory();
		public static Action<Task<bool>> SetResult = CreateResultSetter();

		public StorageWriter(StorageState state)
		{
			_state = state;
		}

		public async Task WriteAsync(WriteBatch batch)
		{
			if (Log.IsDebugEnabled)
				Log.Debug(batch.DebugVal);

			var mine = new OutstandingWrite(batch);
			_pendingWrites.Enqueue(mine);

			var state = new Reference<int>();
			while (mine.Done() == false && _pendingWrites.Peek() != mine)
			{
				if (Log.IsDebugEnabled)
					Log.Debug("Not the only concurrent write for write batch # {0},waiting...", batch.BatchId);
				await _writeCompletedEvent.WaitAsync(state).ConfigureAwait(false);
			}

			if (mine.Done())
			{
				if (Log.IsDebugEnabled)
					Log.Debug("Write batch #{0} was completed early, done (no lockneeded), will pulse.", batch.BatchId);
				_writeCompletedEvent.PulseAll();
				return;
			}

			List<OutstandingWrite> list = null;
			try
			{
				using (AsyncLock.LockScope locker = await
_state.Lock.LockAsync().ConfigureAwait(false))
				{
					if (mine.Done())
					{
						if (Log.IsDebugEnabled)
							Log.Debug("Write batch #{0} was completed early, done (lock wastaken & released).",
									   batch.BatchId);
						return;
					}

					await _state.MakeRoomForWriteAsync(force: false, lockScope:
locker).ConfigureAwait(false);

					ulong lastSequence = _state.VersionSet.LastSequence;

					list = BuildBatchGroup(mine);

					if (list.Count > 1)
					{
						if (Log.IsDebugEnabled)
							Log.Debug("Write batch #{0} will be written along with {1}batches, all at once.",
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

						await WriteBatch.WriteToLogAsync(list.Select(x =>
x.Batch).ToArray(), currentSequence, _state).ConfigureAwait(false);

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
						SetResult(write.Task);
					}
				}

				if (Log.IsDebugEnabled)
					Log.Debug("Pulsing all pending writes from batch #{0}", batch.BatchId);

				_writeCompletedEvent.PulseAll();
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
				Task = StorageWriter.CreateTask();
			}

			public WriteBatch Batch { get; private set; }
			public Task<bool> Task { get; private set; }

			public long Size { get; private set; }

			public bool Done()
			{
				if (Task.IsCompleted)
					return true;
				if (Task.IsCanceled || Task.IsFaulted)
					Task.Wait(); // throws
				return false;
			}
		}

		private static Action<Task<bool>> CreateResultSetter()
		{
			var methodInfo = typeof(Task<bool>).GetMethod("TrySetResult", BindingFlags.NonPublic | BindingFlags.Instance);
			var dynamicMethod = new DynamicMethod("TrySetResult", typeof(void), new Type[] { typeof(Task<bool>) },
												  typeof(Task<bool>), true);
			var ilGenerator = dynamicMethod.GetILGenerator();
			ilGenerator.Emit(OpCodes.Ldarg_0);
			ilGenerator.Emit(OpCodes.Ldc_I4_1);
			ilGenerator.Emit(OpCodes.Call, methodInfo);
			ilGenerator.Emit(OpCodes.Pop);
			ilGenerator.Emit(OpCodes.Ret);

			var setResult = (Action<Task<bool>>)dynamicMethod.CreateDelegate(typeof(Action<Task<bool>>));
			return setResult;
		}

		private static Func<Task<bool>> CreateTaskFactory()
		{
			var constructorInfo = typeof(Task<bool>).GetConstructor(BindingFlags.NonPublic | BindingFlags.Instance, null,
																	 new Type[0], new ParameterModifier[0]);
			var dynamicMethod = new DynamicMethod("CreateTask", typeof(Task<bool>), new Type[0], typeof(Task<bool>), true);
			var ilGenerator = dynamicMethod.GetILGenerator();
			ilGenerator.Emit(OpCodes.Newobj, constructorInfo);
			ilGenerator.Emit(OpCodes.Ret);

			var createTask = (Func<Task<bool>>)dynamicMethod.CreateDelegate(typeof(Func<Task<bool>>));
			return createTask;
		}
	}
}