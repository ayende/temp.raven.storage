namespace Raven.Storage.Impl
{
	using System;

	using Raven.Storage.Data;
	using Raven.Storage.Impl.Streams;

	public static class Snapshot
	{
		public static void Write(LogWriter logWriter, StorageOptions options, VersionSet versionSet)
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
					InternalKey key;
					if(!InternalKey.TryParse(compactionPointer, out key))
						throw new FormatException("Invalid internal key format.");

					edit.SetCompactionPointer(level, key);
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
		}
	}
}