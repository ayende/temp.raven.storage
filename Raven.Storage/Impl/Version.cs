namespace Raven.Storage.Impl
{
	using System;
	using System.Collections.Generic;
	using System.Linq;

	using Raven.Storage.Comparing;
	using Raven.Storage.Data;

	public class Version
	{
		private const int TargetFileSize = 2 * 1048576;

		// Maximum bytes of overlaps in grandparent (i.e., level+2) before we
		// stop building a single file in a level->level+1 compaction.
		private const long MaxGrandParentOverlapBytes = 10 * TargetFileSize;

		private readonly InternalKeyComparator internalKeyComparator;

		public Version(StorageOptions options)
		{
			this.internalKeyComparator = new InternalKeyComparator(options.Comparator);
			this.Files = new List<FileMetadata>[Config.NumberOfLevels];
		}

		public Version(StorageOptions options, VersionSet versionSet)
			: this(options)
		{
			throw new NotImplementedException();
		}

		public int CompactionLevel { get; set; }

		public double CompactionScore { get; set; }

		public FileMetadata FileToCompact { get; set; }

		public List<FileMetadata>[] Files { get; set; }

		public static void Finalize(Version version)
		{
			// Precomputed best level for next compaction
			int bestLevel = -1;
			double bestScore = -1;

			for (var level = 0; level < Config.NumberOfLevels - 1; level++)
			{
				double score;
				if (level == 0)
				{
					// We treat level-0 specially by bounding the number of files
					// instead of number of bytes for two reasons:
					//
					// (1) With larger write-buffer sizes, it is nice not to do too
					// many level-0 compactions.
					//
					// (2) The files in level-0 are merged on every read and
					// therefore we wish to avoid too many files when the individual
					// file size is small (perhaps because of a small write-buffer
					// setting, or very high compression ratios, or lots of
					// overwrites/deletions).
					score = version.Files[level].Count / (double)Config.Level0CompactionTrigger;
				}
				else
				{
					// Compute the ratio of current size to size limit.
					var levelBytes = version.Files[level].Sum(x => x.FileSize);
					score = levelBytes / MaxBytesForLevel(level);
				}

				if (score > bestScore)
				{
					bestLevel = level;
					bestScore = score;
				}
			}

			version.CompactionLevel = bestLevel;
			version.CompactionScore = bestScore;
		}

		private static double MaxBytesForLevel(int level)
		{
			// Note: the result for level zero is not really used since we set
			// the level-0 compaction threshold based on number of files.
			var result = 10 * 1048576.0; // Result for both level-0 and level-1
			while (level > 1)
			{
				result *= 10;
				level--;
			}

			return result;
		}

		public int PickLevelForMemTableOutput(Slice smallestKey, Slice largestKey)
		{
			int level = 0;
			if (!OverlapInLevel(0, smallestKey, largestKey))
			{
				while (level < Config.MaxMemCompactLevel)
				{
					if (OverlapInLevel(level + 1, smallestKey, largestKey))
					{
						break;
					}

					var overlaps = GetOverlappingInputs(level + 2, smallestKey, largestKey);
					var totalFileSize = overlaps.Sum(x => x.FileSize);
					if (totalFileSize > MaxGrandParentOverlapBytes)
					{
						break;
					}

					level++;
				}
			}

			return level;
		}

		private IEnumerable<FileMetadata> GetOverlappingInputs(int level, Slice begin, Slice end)
		{
			var inputs = new List<FileMetadata>();
			var userComparator = internalKeyComparator.UserComparator;

			for (int i = 0; i < Files[level].Count; )
			{
				var f = Files[level][i++];
				var fileStart = f.SmallestKey;
				var fileLimit = f.LargestKey;

				if (userComparator.Compare(fileLimit, begin) < 0)
				{
					// "f" is completely before specified range; skip it
				}
				else if (userComparator.Compare(fileStart, end) > 0)
				{
					// "f" is completely after specified range; skip it
				}
				else
				{
					inputs.Add(f);
					if (level == 0)
					{
						// Level-0 files may overlap each other.  So check if the newly
						// added file has expanded the range.  If so, restart search.
						if (userComparator.Compare(fileStart, begin) < 0)
						{
							begin = fileStart;
							inputs.Clear();
							i = 0;
						}
						else if (userComparator.Compare(fileLimit, end) > 0)
						{
							end = fileLimit;
							inputs.Clear();
							i = 0;
						}
					}
				}
			}

			return inputs;
		}

		private bool OverlapInLevel(int level, Slice smallestKey, Slice largestKey)
		{
			return SomeFileOverlapsRange(level > 0, Files[level], smallestKey, largestKey);
		}

		private bool SomeFileOverlapsRange(bool disjointSortedFiles, IEnumerable<FileMetadata> files, Slice smallestKey, Slice largestKey)
		{
			if (!disjointSortedFiles)
			{
				var userComparator = internalKeyComparator.UserComparator;

				// Need to check against all files
				return files.Any(file => !this.AfterFile(userComparator, smallestKey, file) && !this.BeforeFile(userComparator, largestKey, file));
			}

			return false;
		}

		private bool BeforeFile(IComparator comparator, Slice key, FileMetadata file)
		{
			// NULL 'key' occurs after all keys and is therefore never before 'file'
			return comparator.Compare(key, file.SmallestKey) < 0;
		}

		private bool AfterFile(IComparator comparator, Slice key, FileMetadata file)
		{
			// NULL 'key' occurs before all keys and is therefore never after 'file'
			return comparator.Compare(key, file.LargestKey) > 0;
		}
	}
}