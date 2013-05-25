using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Raven.Storage.Impl
{
	public class AsyncMonitor
	{
		private ConcurrentQueue<TaskCompletionSource<object>> _pending =
			new ConcurrentQueue<TaskCompletionSource<object>>();

		public Task WaitAsync()
		{
			var item = new TaskCompletionSource<object>();
			_pending.Enqueue(item);
			return item.Task;
		}

		public void Pulse()
		{
			TaskCompletionSource<object> source;
			var current = _pending;
			_pending = new ConcurrentQueue<TaskCompletionSource<object>>();
			while (current.TryDequeue(out source))
			{
				source.TrySetResult(null);
			}
		}
	}
}