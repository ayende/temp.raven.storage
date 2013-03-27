using System;

namespace Raven.Storage.Memory
{
	public interface IArrayAccessor : IDisposable
	{
		byte this[long i] { get; }
		long Capacity { get; }
		int ReadInt32(long i);
		int ReadArray(long pos, byte[] buffer, int offset, int count);
	}
}