using System.IO.MemoryMappedFiles;

namespace Raven.Storage.Data
{
	public class FileData
	{
		public FileData(MemoryMappedFile file, long size)
		{
			Size = size;
			File = file;
		}

		public long Size { get; private set; }
		 public MemoryMappedFile File { get; private set; }
	}
}