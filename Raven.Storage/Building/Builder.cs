namespace Raven.Storage.Building
{
	using System;
	using System.Collections.Generic;
	using System.Linq;

	using Raven.Storage.Comparing;
	using Raven.Storage.Impl;

	using Version = Raven.Storage.Impl.Version;

	/// <summary>
	/// A helper class so we can efficiently apply a whole sequence
	/// of edits to a particular state without creating intermediate
	/// Versions that contain full copies of the intermediate state.
	/// </summary>
	public class Builder
	{
		private readonly VersionSet versionSet;

		private readonly LevelState[] levels;

		private readonly BySmallestKey comparator;

		private readonly Version @base;

		private readonly IStorageContext storageContext;

		/// <summary>
		/// Initialize a builder with the files from *base and other info from *vset
		/// </summary>
		public Builder(IStorageContext storageContext, VersionSet versionSet, Version @base)
		{
			this.storageContext = storageContext;
			this.versionSet = versionSet;
			this.@base = @base;
			comparator = new BySmallestKey(storageContext.InternalKeyComparator);

			levels = new LevelState[Config.NumberOfLevels];
			for (var level = 0; level < Config.NumberOfLevels; level++)
			{
				levels[level] = new LevelState();
			}
		}

		/// <summary>
		/// Apply all of the edits in *edit to the current state.
		/// </summary>
		/// <param name="edit"></param>
		public void Apply(VersionEdit edit)
		{
			for (var level = 0; level < Config.NumberOfLevels; level++)
			{
				if (edit.CompactionPointers[level].Count <= 0)
				{
					continue;
				}

				var last = edit.CompactionPointers[level].Last();
				versionSet.CompactionPointers[level] = last.Encode();
			}

			var deletedFiles = new Dictionary<int, IList<ulong>>(edit.DeletedFiles);
			for (var level = 0; level < Config.NumberOfLevels; level++)
			{
				foreach (var fileNumber in deletedFiles[level])
				{
					levels[level].DeletedFiles.Add(fileNumber);
				}
			}

			for (var level = 0; level < Config.NumberOfLevels; level++)
			{
				foreach (var newFile in edit.NewFiles[level])
				{
					var file = new FileMetadata(newFile);

					// We arrange to automatically compact this file after
					// a certain number of seeks.  Let's assume:
					//   (1) One seek costs 10ms
					//   (2) Writing or reading 1MB costs 10ms (100MB/s)
					//   (3) A compaction of 1MB does 25MB of IO:
					//         1MB read from this level
					//         10-12MB read from next level (boundaries may be misaligned)
					//         10-12MB written to next level
					// This implies that 25 seeks cost the same as the compaction
					// of 1MB of data.  I.e., one seek costs approximately the
					// same as the compaction of 40KB of data.  We are a little
					// conservative and allow approximately one seek for every 16KB
					// of data before triggering a compaction.

					file.AllowedSeeks = file.FileSize / 16384;
					if (file.AllowedSeeks < 100)
					{
						file.AllowedSeeks = 100;
					}

					levels[level].DeletedFiles.Remove(file.FileNumber);
					levels[level].AddedFiles.Add(file);
				}
			}
		}

		public void SaveTo(Version version)
		{
			for (var level = 0; level < Config.NumberOfLevels; level++)
			{
				// Merge the set of added files with the set of pre-existing files.
				// Drop any deleted files.  Store the result in *v.
				var baseFiles = @base.Files[level];
				var addedFiles = levels[level].AddedFiles;

				var result = new List<FileMetadata>();
				result.AddRange(addedFiles);
				result.AddRange(baseFiles);

				foreach (var file in result.OrderBy(x => x, comparator))
				{
					MaybeAddFile(version, level, file);
				}
			}
		}

		private void MaybeAddFile(Version version, int level, FileMetadata file)
		{
			if (levels[level].DeletedFiles.Contains(file.FileNumber))
			{
				return;
			}

			if (level > 0 && version.Files[level].Count > 0)
			{
				// Must not overlap
				if (storageContext.InternalKeyComparator.Compare(version.Files[level].Last().LargestKey, file.SmallestKey) >= 0)
				{
					throw new InvalidOperationException("Files overlap.");
				}
			}

			version.Files[level].Add(file);
		}

		internal class BySmallestKey : IComparer<FileMetadata>
		{
			private readonly InternalKeyComparator comparator;

			public BySmallestKey(InternalKeyComparator comparator)
			{
				this.comparator = comparator;
			}

			public int Compare(FileMetadata file1, FileMetadata file2)
			{
				var result = comparator.Compare(file1.SmallestKey, file2.SmallestKey);
				if (result != 0)
				{
					return result;
				}

				// Break ties by file number
				if (file1.FileNumber < file2.FileNumber)
				{
					return -1;
				}

				if (file1.FileNumber > file2.FileNumber)
				{
					return 1;
				}

				return 0;
			}
		}

		internal class LevelState
		{
			public LevelState()
			{
				DeletedFiles = new List<ulong>();
				AddedFiles = new List<FileMetadata>();
			}

			public IList<ulong> DeletedFiles { get; private set; }

			public IList<FileMetadata> AddedFiles { get; private set; }
		}
	}
}