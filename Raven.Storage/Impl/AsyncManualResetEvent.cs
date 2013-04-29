using System.Threading;
using System.Threading.Tasks;

namespace Raven.Storage.Impl
{
	public class AsyncManualResetEvent
	{
		private volatile TaskCompletionSource<bool> _tcs = new TaskCompletionSource<bool>();

		public Task WaitAsync()
		{
			return _tcs.Task;
		}

		public void Set()
		{
			_tcs.TrySetResult(true);
		}

		public void Reset()
		{
			while (true)
			{
				var tcs = _tcs;
				if (!tcs.Task.IsCompleted)
					return;
				
#pragma warning disable 420
				if(Interlocked.CompareExchange(ref this._tcs, new TaskCompletionSource<bool>(), tcs) == tcs)
					return;
#pragma warning restore 420
			}
		}
	}
}