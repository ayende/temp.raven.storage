using System.IO.MemoryMappedFiles;
using Raven.Storage.Memory;

namespace Raven.Storage.Data
{
	public class FileData
	{
		public FileData(IAccessor file, long size)
		{
			Size = size;
			File = file;
		}

		public long Size { get; private set; }
		public IAccessor File { get; private set; }
	}
}