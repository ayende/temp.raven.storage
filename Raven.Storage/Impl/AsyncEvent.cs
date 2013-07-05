using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Raven.Storage.Impl
{
    public class AsyncEvent : IDisposable
    {
        private int _state;
        private volatile bool _disposed;
        private readonly object _locker = new object();
        private LinkedList<Task<bool>> _pending = new LinkedList<Task<bool>>();

        public async Task<bool> WaitAsync(Reference<int> callerState)
        {
            if (_disposed)
                return false;
            var localState = _state;
            if (callerState.Value != localState)
            {
                callerState.Value = localState;
                return false;
            }

            Task<bool> task;
            lock (_locker)
            {
                if (callerState.Value != _state)
                {
                    callerState.Value = _state;
                    return false;
                }

	            task = StorageWriter.CreateTask();
                _pending.AddLast(task);

                callerState.Value = _state;
            }
            await task.ConfigureAwait(false);
            return true;

        }

        public void Dispose()
        {
            _disposed = true;
            PulseAll();
        }

        public void PulseAll()
        {
            LinkedList<Task<bool>> current;
            lock (_locker)
            {
                _state++;
                current = _pending;
                _pending = new LinkedList<Task<bool>>();
            }

            foreach (var source in current)
            {
	            StorageWriter.SetResult(source);
            }
        }
    }
}