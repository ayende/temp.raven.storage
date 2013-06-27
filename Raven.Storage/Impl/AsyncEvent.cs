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
        private LinkedList<TaskCompletionSource<object>> _pending = new LinkedList<TaskCompletionSource<object>>();

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

            TaskCompletionSource<object> taskCompletionSource;
            lock (_locker)
            {
                if (callerState.Value != _state)
                {
                    callerState.Value = _state;
                    return false;
                }

                taskCompletionSource = new TaskCompletionSource<object>();
                _pending.AddLast(taskCompletionSource);

                callerState.Value = _state;
            }
            await taskCompletionSource.Task.ConfigureAwait(false);
            return true;

        }

        public void Dispose()
        {
            _disposed = true;
            PulseAll();
        }

        public void PulseAll()
        {
            LinkedList<TaskCompletionSource<object>> current;
            lock (_locker)
            {
                _state++;
                current = _pending;
                _pending = new LinkedList<TaskCompletionSource<object>>();
            }

            foreach (var source in current)
            {
                source.TrySetResult(null);
            }
        }
    }
}