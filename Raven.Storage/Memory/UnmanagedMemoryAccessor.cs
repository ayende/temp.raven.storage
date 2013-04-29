using System;
using System.Collections.Generic;
using System.IO;
using System.Linq.Expressions;
using System.Runtime.InteropServices;
using System.Threading;
using Raven.Storage.Util;
using System.Linq;

namespace Raven.Storage.Memory
{
	/// <summary>
	/// Provide access to unmanaged memory
	/// Note that this class assume a single writer and multiple readers
	/// </summary>
	public class UnamangedMemoryAccessor : IDisposable
	{
		private UnamagedMemory[] _memHandles = new UnamagedMemory[1];
		private readonly byte[] _scratch = new byte[1024*4];

		private class UnamagedMemory
		{
			public IntPtr Ptr;
			public int Remaining;
			public int Position;
			public int Size;
		}

		public UnamangedMemoryAccessor(int size)
		{
			size = Info.GetPowerOfTwo(size);// we ensure power of 2 to make sure that we don't fragment the _unmanaged_ heap
			IntPtr allocHGlobal;
			try
			{
				allocHGlobal = Marshal.AllocHGlobal(size);
			}
			catch (Exception)
			{
				GC.SuppressFinalize(this);
				throw;
			}
			_memHandles[0] = new UnamagedMemory
				{
					Ptr = allocHGlobal,
					Remaining = size,
					Size = size
				};
		}

		public long OverallMemoryUsage
		{
			get { return _memHandles.Sum(x => x.Size); }
		}

		public unsafe Stream Read(MemoryHandle handle)
		{
			var unamagedMemory = _memHandles[handle.Index];
			var pos = unamagedMemory.Ptr + handle.Pos;
			return new UnmanagedMemoryStream((byte*) pos, handle.Size, handle.Size, FileAccess.Read);
		}

		public MemoryHandle Write(Stream stream)
		{
			var memIndex = GetIndexOfHandleWithEnoughSpace(stream);
			var unamagedMemory = _memHandles[memIndex];

			var handle = new MemoryHandle
				{
					Pos = unamagedMemory.Position,
					Index = memIndex,
					Size = (int) stream.Length
				};
			while (true)
			{
				var reads = stream.Read(_scratch, 0, _scratch.Length);
				if (reads == 0)
					break;
				Marshal.Copy(_scratch, 0, unamagedMemory.Ptr + unamagedMemory.Position, reads);
				unamagedMemory.Position += reads;
			}
			return handle;
		}

		public class MemoryHandle
		{
			public int Index;
			public int Size;
			public int Pos;
		}

		private int GetIndexOfHandleWithEnoughSpace(Stream stream)
		{
			int memIndex = 0;
			var length = (int) stream.Length;
			for (; memIndex < _memHandles.Length; memIndex++)
			{
				if (_memHandles[memIndex].Remaining < length)
					return memIndex;
			}
			// couldn't find any space left, need to allocate more
			// this should only happen _once_, since afterward, we would freeze
			// the mem table. The code is ready to handle multiple uses, none the less
			
			var newMemHandles = new UnamagedMemory[memIndex + 1];
			Array.Copy(_memHandles, 0, newMemHandles, 0, _memHandles.Length);
			var size = Info.GetPowerOfTwo(length);// we ensure power of 2 to make sure that we don't fragment the _unmanaged_ heap
			var newMemHandle = new UnamagedMemory
				{
					Remaining = size,
					Size = size
				};
			newMemHandles[memIndex] = newMemHandle;
			try
			{
				newMemHandle.Ptr = Marshal.AllocHGlobal(size);
				Volatile.Write(ref _memHandles, newMemHandles);
			}
			catch (Exception)
			{
				if (newMemHandle.Ptr != IntPtr.Zero)
					Marshal.FreeHGlobal(newMemHandle.Ptr);
				throw;
			}
			return memIndex;
		}

		public void Dispose()
		{
			foreach (var unamagedMemory in _memHandles)
			{
				if (unamagedMemory.Ptr != IntPtr.Zero)
					Marshal.FreeHGlobal(unamagedMemory.Ptr);
			}
			GC.SuppressFinalize(this);
		}

		~UnamangedMemoryAccessor()
		{
			Dispose();
		}
	}
}