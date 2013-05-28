namespace Raven.Storage.Impl
{
	public static class Config
	{
		/// <summary>
		/// Soft limit on number of level-0 files.  We slow down writes at this point.
		/// </summary>
		public const int SlowdownWritesTrigger = 8;

		/// <summary>
		/// Maximum number of level-0 files.  We stop writes at this point.
		/// </summary>
		public const int StopWritesTrigger = 12;

		/// <summary>
		/// Header is checksum (4 bytes), type (1 byte), length (2 bytes).
		/// </summary>
		public const int HeaderSize = 4 + 1 + 2;

		public const int NumberOfLevels = 7;

		/// <summary>
		/// Level-0 compaction is started when we hit this many files.
		/// </summary>
		public const int Level0CompactionTrigger = 4;

		/// <summary>
		/// Maximum level to which a new compacted memtable is pushed if it
		/// does not create overlap.  We try to push to level 2 to avoid the
		/// relatively expensive level 0=>1 compactions and to avoid some
		/// expensive manifest file operations.  We do not push all the way to
		/// the largest level since that can generate a lot of wasted disk
		/// space if the same key space is being repeatedly overwritten.
		/// </summary>
		public const int MaxMemCompactLevel = 2;

		public const int TargetFileSize = 2 * 1048576;

		// Maximum bytes of overlaps in grandparent (i.e., level+2) before we
		// stop building a single file in a level->level+1 compaction.
		public const long MaxGrandParentOverlapBytes = 10 * TargetFileSize;
	}
}