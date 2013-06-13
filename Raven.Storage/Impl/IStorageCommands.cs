namespace Raven.Storage.Impl
{
	using System.Threading.Tasks;

	using Raven.Storage.Data;

	public interface IStorageCommands
	{
		void Compact(int level, Slice begin, Slice end);

		Task CompactAsync(int level, Slice begin, Slice end);

		void CompactRange(Slice begin, Slice end);

		Task CompactRangeAsync(Slice begin, Slice end);

		Snapshot CreateSnapshot();

		Task<Snapshot> CreateSnapshotAsync();

		void ReleaseSnapshot(Snapshot snapshot);

		Task ReleaseSnapshotAsync(Snapshot snapshot);
	}
}