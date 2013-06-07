using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Raven.Storage.Impl
{
	public class AsyncLock : IDisposable
	{
		private readonly object locker = new object();
		private readonly Queue<TaskCompletionSource<object>> waiters = new Queue<TaskCompletionSource<object>>();
		private bool locked;

		public void Exit()
		{
			lock (locker)
			{
				locked = false;
				if (waiters.Count == 0)
					return;
				var taskCompletionSource = waiters.Dequeue();
				taskCompletionSource.SetResult(null);
			}
		}

		public async Task<LockScope> LockAsync()
		{
			TaskCompletionSource<object> taskCompletionSource;
			lock (locker)
			{
				if (locked == false)
				{
					locked = true;
					return new LockScope(this);
				}

				taskCompletionSource = new TaskCompletionSource<object>();
				waiters.Enqueue(taskCompletionSource);
			}

			await taskCompletionSource.Task;
			lock (locker)
			{
				locked = true;
				return new LockScope(this);
			}
		}

		public void Dispose()
		{
		}

		public class LockScope : IDisposable
		{
			private bool locked;
			private readonly AsyncLock _asyncLock;

			public LockScope(AsyncLock asyncLock)
			{
				_asyncLock = asyncLock;
				locked = true;
			}

			public void Dispose()
			{
				if (locked)
					_asyncLock.Exit();
				locked = false;
			}

			public void Exit()
			{
				_asyncLock.Exit();
				locked = false;
			}

			public async Task LockAsync()
			{
				if (locked)
					return;
				await _asyncLock.LockAsync();
				locked = true;
			}
		}
	}
}