namespace Raven.Storage.Impl.Compactions
{
	public struct CompactionStats
	{
		public long Micros { get; set; }

		public long BytesRead { get; set; }

		public long BytesWritten { get; set; }

		public void Add(CompactionStats compactionStats)
		{
			Micros += compactionStats.Micros;
			BytesRead += compactionStats.BytesRead;
			BytesWritten += compactionStats.BytesWritten;
		}
	}
}