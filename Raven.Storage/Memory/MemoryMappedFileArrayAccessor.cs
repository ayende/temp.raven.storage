using System.IO.MemoryMappedFiles;

namespace Raven.Storage.Memory
{
	public class MemoryMappedFileArrayAccessor : IArrayAccessor
	{
		private readonly MemoryMappedViewAccessor _accessor;

		public MemoryMappedFileArrayAccessor(MemoryMappedViewAccessor accessor)
		{
			_accessor = accessor;
		}

		public byte this[long i]
		{
			get { return _accessor.ReadByte(i); }
		}

		public long Capacity { get { return _accessor.Capacity; } }

		public int ReadInt32(long i)
		{
			return _accessor.ReadInt32(i);
		}

		public int ReadArray(long pos, byte[] buffer, int offset, int count)
		{
			return _accessor.ReadArray(pos, buffer, offset, count);
		}

		public void Dispose()
		{
			_accessor.Dispose();
		}
	}
}