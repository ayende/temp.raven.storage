using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace Raven.Storage.Impl
{
	public class AsyncEvent : IDisposable
	{
		private int state;
		private volatile bool disposed;
		private readonly object locker = new object();

		public bool Wait(Reference<int> callerState)
		{
			if (disposed)
				return false;
			var localState = state;
			if (callerState.Value != localState)
			{
				callerState.Value = localState;
				return false;
			}
			lock (locker)
			{
				if (callerState.Value != state)
				{
					callerState.Value = state;
					return false;
				}

				Monitor.Wait(locker);
				callerState.Value = state;
				return true;
			}
		}

		public void Dispose()
		{
			disposed = true;
			PulseAll();
		}

		public void PulseAll()
		{
			lock (locker)
			{
				state++;
				Monitor.PulseAll(locker);
			}
		}
	}
}