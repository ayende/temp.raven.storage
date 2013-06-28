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

			await taskCompletionSource.Task.ConfigureAwait(false);
			lock (locker)
			{
				locked = true;
				return new LockScope(this);
			}
		}

		public void Dispose()
		{
			lock (locker)
			{
				foreach (var taskCompletionSource in waiters)
				{
					taskCompletionSource.SetResult(null);
				}
				waiters.Clear();
			}
		}

		public class LockScope : IDisposable
		{
			private readonly AsyncLock _asyncLock;
			private bool _locked;

			public LockScope(AsyncLock asyncLock)
			{
				_asyncLock = asyncLock;
				_locked = true;
			}

			public bool Locked
			{
				get { return _locked; }
			}

			public void Dispose()
			{
				if (_locked)
					_asyncLock.Exit();
				_locked = false;
			}

			public void Exit()
			{
				_asyncLock.Exit();
				_locked = false;
			}

			public async Task<LockScope> LockAsync()
			{
				if (_locked)
					return this;
				
				await _asyncLock.LockAsync().ConfigureAwait(false);
				_locked = true;

				return this;
			}
		}
	}
}