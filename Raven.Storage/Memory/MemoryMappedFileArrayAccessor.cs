using System.IO.MemoryMappedFiles;

namespace Raven.Storage.Memory
{
	public class MemoryMappedFileArrayAccessor : IArrayAccessor
	{
		private readonly string _name;
		private readonly MemoryMappedViewAccessor _accessor;

		public MemoryMappedFileArrayAccessor(string name, MemoryMappedViewAccessor accessor)
		{
			_name = name;
			_accessor = accessor;
		}

		public override string ToString()
		{
			return string.Format("Name: {0}", _name);
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