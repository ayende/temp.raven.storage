namespace Raven.Storage.Impl
{
	using System.Threading.Tasks;

	using Raven.Storage.Data;

	public class StorageCommands : IStorageCommands
	{
		private readonly StorageState state;

		public StorageCommands(StorageState state)
		{
			this.state = state;
		}

		public async Task CompactAsync(int level, Slice begin, Slice end)
		{
			using (await state.Lock.LockAsync())
			{
				await state.Compactor.CompactAsync(level, begin, end, state.Lock);
			}
		}

		public Task CompactRangeAsync(Slice begin, Slice end)
		{
			return state.Compactor.CompactRangeAsync(begin, end);
		}

		public async Task<Snapshot> CreateSnapshotAsync()
		{
			using (var locker = await state.Lock.LockAsync())
			{
				return await state.Snapshooter.CreateNewSnapshotAsync(this.state.VersionSet, locker);
			}
		}

		public async Task ReleaseSnapshotAsync(Snapshot snapshot)
		{
			using (var locker = await state.Lock.LockAsync())
			{
				await state.Snapshooter.DeleteAsync(snapshot, locker);
			}
		}
	}
}