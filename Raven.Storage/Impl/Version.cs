namespace Raven.Storage.Impl
{
	using System;
	using System.Collections.Generic;
	using System.Linq;

	using Raven.Storage.Comparing;
	using Raven.Storage.Data;

	public class Version
	{
		private readonly VersionSet versionSet;

		public Version()
		{
			this.Files = new List<FileMetadata>[Config.NumberOfLevels];
		}

		public Version(VersionSet versionSet)
			: this()
		{
			this.versionSet = versionSet;
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

					
				}
			}

			return level;
		}

		private bool OverlapInLevel(int level, Slice smallestKey, Slice largestKey)
		{
			
		}
	}
}