namespace Raven.Storage.Impl
{
	using System;
	using System.Collections.Generic;
	using System.IO;
	using System.Linq;

	using Raven.Storage.Comparing;
	using Raven.Storage.Data;
	using Raven.Storage.Impl.Caching;
	using Raven.Storage.Reading;
	using Raven.Storage.Util;

	public class Version
	{
		private readonly IStorageContext storageContext;

		private Version(IStorageContext storageContext)
		{
			this.storageContext = storageContext;
			Files = new List<FileMetadata>[Config.NumberOfLevels];

			FileToCompact = null;
			FileToCompactLevel = -1;

			CompactionScore = -1;
			CompactionLevel = -1;

			for (var level = 0; level < Config.NumberOfLevels; level++)
			{
				Files[level] = new List<FileMetadata>();
			}
		}

		public Version(IStorageContext storageContext, VersionSet versionSet)
			: this(storageContext)
		{
			VersionSet = versionSet;
		}

		public int CompactionLevel { get; private set; }

		public double CompactionScore { get; private set; }

		public int FileToCompactLevel { get; private set; }

		public FileMetadata FileToCompact { get; private set; }

		public List<FileMetadata>[] Files { get; private set; }

		public VersionSet VersionSet { get; private set; }

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
				var start = new InternalKey(smallestKey, Format.MaxSequenceNumber, ItemType.ValueForSeek);
				var limit = new InternalKey(largestKey, 0, 0);
				while (level < Config.MaxMemCompactLevel)
				{
					if (OverlapInLevel(level + 1, smallestKey, largestKey))
					{
						break;
					}

					var overlaps = GetOverlappingInputs(level + 2, start, limit);
					var totalFileSize = overlaps.Sum(x => x.FileSize);
					if (totalFileSize > Config.MaxGrandParentOverlapBytes)
					{
						break;
					}

					level++;
				}
			}

			return level;
		}

		internal List<FileMetadata> GetOverlappingInputs(int level, InternalKey begin, InternalKey end)
		{
			var inputs = new List<FileMetadata>();
			var userComparator = storageContext.InternalKeyComparator.UserComparator;

			var userBegin = begin.UserKey;
			var userEnd = end.UserKey;

			for (int i = 0; i < Files[level].Count; )
			{
				var f = Files[level][i++];
				var fileStart = f.SmallestKey.UserKey;
				var fileLimit = f.LargestKey.UserKey;

				if (userComparator.Compare(fileLimit, userBegin) < 0)
				{
					// "f" is completely before specified range; skip it
				}
				else if (userComparator.Compare(fileStart, userEnd) > 0)
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
						if (userComparator.Compare(fileStart, userBegin) < 0)
						{
							userBegin = fileStart;
							inputs.Clear();
							i = 0;
						}
						else if (userComparator.Compare(fileLimit, userEnd) > 0)
						{
							userEnd = fileLimit;
							inputs.Clear();
							i = 0;
						}
					}
				}
			}

			return inputs;
		}

		internal bool OverlapInLevel(int level, Slice smallestKey, Slice largestKey)
		{
			return SomeFileOverlapsRange(level > 0, Files[level], smallestKey, largestKey);
		}

		private bool SomeFileOverlapsRange(bool disjointSortedFiles, IEnumerable<FileMetadata> files, Slice smallestKey, Slice largestKey)
		{
			if (!disjointSortedFiles)
			{
				var userComparator = storageContext.InternalKeyComparator.UserComparator;

				// Need to check against all files
				return files.Any(file => !AfterFile(userComparator, smallestKey, file) && !BeforeFile(userComparator, largestKey, file));
			}

			return false;
		}

		private bool BeforeFile(IComparator comparator, Slice key, FileMetadata file)
		{
			// NULL 'key' occurs after all keys and is therefore never before 'file'
			return comparator.Compare(key, file.SmallestKey.UserKey) < 0;
		}

		private bool AfterFile(IComparator comparator, Slice key, FileMetadata file)
		{
			// NULL 'key' occurs before all keys and is therefore never after 'file'
			return comparator.Compare(key, file.LargestKey.UserKey) > 0;
		}

		public bool UpdateStats(GetStats stats)
		{
			var file = stats.SeekFile;
			if (file != null)
			{
				file.AllowedSeeks--;
				if (file.AllowedSeeks <= 0 && FileToCompact == null)
				{
					FileToCompact = file;
					FileToCompactLevel = stats.SeekFileLevel;
				}
			}

			return false;
		}

		public bool TryGet(Slice key, ulong seq, ReadOptions readOptions, out Stream stream, out GetStats stats)
		{
			stats = new GetStats
						{
							SeekFile = null,
							SeekFileLevel = -1
						};

			FileMetadata lastFileRead = null;
			int lastFileReadLevel = -1;

			var internalKey = new InternalKey(key, seq, ItemType.ValueForSeek);

			// We can search level-by-level since entries never hop across
			// levels.  Therefore we are guaranteed that if we find data
			// in an smaller level, later levels are irrelevant.
			for (var level = 0; level < Config.NumberOfLevels; level++)
			{
				if (Files[level].Count == 0)
				{
					continue;
				}

				// Get the list of files to search in this level
				IList<FileMetadata> files = Files[level];
				if (level == 0)
				{
					// Level-0 files may overlap each other.  Find all files that
					// overlap user_key and process them in order from newest to oldest.
					var tempFiles =
						files.Where(
							f =>
							storageContext.InternalKeyComparator.UserComparator.Compare(internalKey.UserKey, f.SmallestKey.UserKey) >= 0
							&& storageContext.InternalKeyComparator.UserComparator.Compare(internalKey.UserKey, f.LargestKey.UserKey) <= 0)
							 .OrderByDescending(x => x.FileNumber);

					if (!tempFiles.Any())
					{
						continue;
					}

					files = tempFiles.ToList();
				}
				else
				{
					// Binary search to find earliest index whose largest key >= ikey.
					int index;
					if (Files[level].TryFindFile(internalKey.TheInternalKey, storageContext.InternalKeyComparator, out index) == false)
					{
						files = new List<FileMetadata>();
					}
					else
					{
						files = storageContext.InternalKeyComparator.UserComparator.Compare(internalKey.UserKey, files[index].SmallestKey.UserKey) < 0 ? new List<FileMetadata>() : files.Skip(index).ToList();
					}
				}

				foreach (var f in files)
				{
					if (lastFileRead != null && stats.SeekFile == null)
					{
						// We have had more than one seek for this read.  Charge the 1st file.
						stats.SeekFile = lastFileRead;
						stats.SeekFileLevel = lastFileReadLevel;
					}

					lastFileRead = f;
					lastFileReadLevel = level;

					var state = storageContext.TableCache.Get(
						internalKey, 
						f.FileNumber, 
						f.FileSize, 
						readOptions,
						storageContext.InternalKeyComparator.UserComparator,
						out stream);

					switch (state)
					{
						case ItemState.Found:
							return true;
						case ItemState.NotFound:
							break;
						case ItemState.Deleted:
							return false;
						case ItemState.Corrupt:
							return false;
						default:
							throw new NotSupportedException(state.ToString());
					}
				}
			}

			stream = null;
			return false;
		}
	}
}