namespace Raven.Storage.Impl
{
	using Raven.Storage.Impl.Streams;

	public static class Snapshot
	{
		public static Status Write(LogWriter logWriter, StorageOptions options, VersionSet versionSet)
		{
			// Save metadata 
			var edit = new VersionEdit();
			edit.SetComparatorName(options.Comparator.Name);

			// Save compaction pointers
			for (int level = 0; level < Config.NumberOfLevels; level++)
			{
				var compactionPointer = versionSet.CompactionPointers[level];
				if (!compactionPointer.IsEmpty())
				{
					edit.SetCompactionPointer(level, compactionPointer);
				}
			}

			// Save files
			for (int level = 0; level < Config.NumberOfLevels; level++)
			{
				var files = versionSet.Current.Files[level];
				foreach (var file in files)
				{
					edit.AddFile(level, file);
				}
			}

			edit.EncodeTo(logWriter);

			return Status.OK();
		}
	}
}