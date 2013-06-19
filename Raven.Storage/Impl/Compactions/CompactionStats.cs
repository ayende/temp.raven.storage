namespace Raven.Storage.Impl.Compactions
{
	public struct CompactionStats
	{
		public long Milliseconds { get; set; }

		public long BytesRead { get; set; }

		public long BytesWritten { get; set; }

		public void Add(CompactionStats compactionStats)
		{
			Milliseconds += compactionStats.Milliseconds;
			BytesRead += compactionStats.BytesRead;
			BytesWritten += compactionStats.BytesWritten;
		}
	}
}