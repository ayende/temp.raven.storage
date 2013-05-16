using System.Collections.Generic;

namespace Raven.Storage.Impl
{
	public class Version
	{
		public int CompactionScore { get; set; }

		public FileMetadata FileToCompact { get; set; }

		public List<FileMetadata>[] Files { get; set; }
	}

	public class FileMetadata
	{
	}
}