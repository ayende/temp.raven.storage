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
			Task compactAsync;
			using (await state.Lock.LockAsync())
			{
				compactAsync = state.Compactor.CompactAsync(level, begin, end, state.Lock);
			}
			await compactAsync; // we do the wait _outside_ the lock

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