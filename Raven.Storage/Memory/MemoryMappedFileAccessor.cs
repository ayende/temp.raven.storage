using System.IO;
using System.IO.MemoryMappedFiles;
using Raven.Storage.Util;

namespace Raven.Storage.Memory
{
	public class MemoryMappedFileAccessor : IAccessor
	{
		private readonly string _name;
		private readonly MemoryMappedFile _mappedFile;

		public MemoryMappedFileAccessor(string name, MemoryMappedFile mappedFile)
		{
			_name = name;
			_mappedFile = mappedFile;
		}

		public override string ToString()
		{
			return string.Format("Name: {0}", _name);
		}

		public IArrayAccessor CreateAccessor(long pos, long count)
		{
			var memoryMappedViewAccessor = _mappedFile.CreateViewAccessor(pos, count, MemoryMappedFileAccess.Read);
			TrackResourceUsage.Track(() => memoryMappedViewAccessor.SafeMemoryMappedViewHandle);
			return new MemoryMappedFileArrayAccessor(_name,memoryMappedViewAccessor);
		}

		public Stream CreateStream(long pos, long count)
		{
			var memoryMappedViewStream = _mappedFile.CreateViewStream(pos, count, MemoryMappedFileAccess.Read);
			TrackResourceUsage.Track(() => memoryMappedViewStream.SafeMemoryMappedViewHandle);
			return memoryMappedViewStream;
		}

		public void Dispose()
		{
			_mappedFile.Dispose();
		}
	}
}