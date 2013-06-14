using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Runtime.InteropServices;
using Microsoft.Win32.SafeHandles;

namespace Raven.Storage.Util
{
	public static class TrackResourceUsage
	{
		private class Usage
		{
			public Func<SafeHandle> Try;
			public string Trace;
		}

		private static readonly ConcurrentQueue<Usage> queue = new ConcurrentQueue<Usage>();

		[Conditional("RESOURCES")]
		public static void Track(Func<SafeHandle> trying)
		{
			queue.Enqueue(new Usage
				{
					Try = trying,
					Trace = new StackTrace(true).ToString()
				});
		}

		[Conditional("RESOURCES")]
		public static void Pending()
		{
			foreach (var usage in queue)
			{
				try
				{
					var safeHandle = usage.Try();
					if (safeHandle.IsClosed || safeHandle.IsInvalid)
						continue;
				}
				catch (ObjectDisposedException)
				{
					continue;
				}
				throw new InvalidOperationException("Resource leak at " + usage.Trace);
			}

		}
	}
}