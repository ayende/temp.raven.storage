﻿using System.Collections.Concurrent;
using System.Diagnostics;
using System.Threading;

namespace Raven.Storage.Util
{
	public class BufferPool
	{
		private readonly ConcurrentDictionary<int, ConcurrentQueue<byte[]>> _bufferPoolBySize = new ConcurrentDictionary<int, ConcurrentQueue<byte[]>>();
		private long _heldSize;

		public long HeldSize
		{
			get { return _heldSize; }
		}

		public byte[] Take(int size)
		{
			size = Info.GetPowerOfTwo(size);
			ConcurrentQueue<byte[]> queue;
			byte[] bytes;
			if (_bufferPoolBySize.TryGetValue(size, out queue) == false || 
				queue.TryDequeue(out bytes) == false)
				return new byte[size];

			Interlocked.Add(ref _heldSize, -size);
			return bytes;
		}

		public void Return(byte[] array)
		{
			var size = Info.GetPowerOfTwo(array.Length);
			Debug.Assert(size == array.Length);// otherwise, probably not a buffer pool buffer

			var queue = _bufferPoolBySize.GetOrAdd(size, CreateNewQueue);
			queue.Enqueue(array);
			Interlocked.Add(ref _heldSize, size);
		}

		public void Clear()
		{
			_bufferPoolBySize.Clear();
			Volatile.Write(ref _heldSize, 0);
		}

		private ConcurrentQueue<byte[]> CreateNewQueue(int _)
		{
			return new ConcurrentQueue<byte[]>();
		}
	}
}