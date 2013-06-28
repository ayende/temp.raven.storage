namespace Raven.Storage.Impl
{
	using System.Threading.Tasks;

	using Raven.Storage.Data;

	public interface IStorageCommands
	{
		Task CompactAsync(int level, Slice begin, Slice end);

		Task CompactRangeAsync(Slice begin, Slice end);

		Task CompactMemTableAsync();

		Task<StorageStatistics> GetStatisticsAsync();

		Snapshot CreateSnapshot();

		void ReleaseSnapshot(Snapshot snapshot);
	}
}