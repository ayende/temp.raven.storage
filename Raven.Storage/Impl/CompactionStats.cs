namespace Raven.Storage.Impl
{
	public struct CompactionStats
	{
		public long Micros { get; set; }

		public long BytesRead { get; set; }

		public long BytesWritten { get; set; }

		public void Add(CompactionStats compactionStats)
		{
			this.Micros += compactionStats.Micros;
			this.BytesRead += compactionStats.BytesRead;
			this.BytesWritten += compactionStats.BytesWritten;
		}
	}
}