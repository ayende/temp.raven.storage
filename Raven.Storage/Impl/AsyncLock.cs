using System;
using System.Threading;
using System.Threading.Tasks;

namespace Raven.Storage.Impl
{
	public class AsyncLock : IDisposable
	{
		private readonly SemaphoreSlim _semaphoreSlim = new SemaphoreSlim(1, 1);
		
		public void Exit()
		{
			_semaphoreSlim.Release();
		}

		public async Task<LockScope> LockAsync()
		{
			await _semaphoreSlim.WaitAsync();
			return new LockScope(this);
		}
		
		public void Dispose()
		{
			_semaphoreSlim.Dispose();
		}

		public class LockScope: IDisposable
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
				await _asyncLock._semaphoreSlim.WaitAsync();
				locked = true;
			}
		}

		public async Task WaitReleaseAsync()
		{
			await _semaphoreSlim.WaitAsync();
			_semaphoreSlim.Release();
		}
	}
}