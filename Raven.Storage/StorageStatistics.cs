namespace Raven.Storage
{
	using System.Collections.Generic;

	using Raven.Storage.Impl;

	public class StorageStatistics
	{
		public StorageStatistics(List<FileMetadata>[] files)
		{
			Files = files;
		}

		public IReadOnlyList<FileMetadata>[] Files { get; private set; }
	}
}