using System.IO;
using System.IO.MemoryMappedFiles;

namespace Raven.Storage.Memory
{
	public class MemoryMappedFileAccessor : IAccessor
	{
		private readonly MemoryMappedFile _mappedFile;

		public MemoryMappedFileAccessor(MemoryMappedFile mappedFile)
		{
			_mappedFile = mappedFile;
		}

		public IArrayAccessor CreateAccessor(long pos, long count)
		{
			return new MemoryMappedFileArrayAccessor(_mappedFile.CreateViewAccessor(pos, count, MemoryMappedFileAccess.Read));
		}

		public Stream CreateStream(long pos, long count)
		{
			return _mappedFile.CreateViewStream(pos, count, MemoryMappedFileAccess.Read);
		}

		public void Dispose()
		{
			_mappedFile.Dispose();
		}
	}
}