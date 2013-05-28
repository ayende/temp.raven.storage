namespace Raven.Storage.Impl
{
	public struct CompactionState
	{
		private readonly Compaction compaction;

		public CompactionState(Compaction compaction)
		{
			this.compaction = compaction;
		}
	}
}