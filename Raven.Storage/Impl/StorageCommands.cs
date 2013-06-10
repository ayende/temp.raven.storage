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

		public void Compact(int level, Slice begin, Slice end)
		{
			CompactAsync(level, begin, end).Wait();
		}

		public Task CompactAsync(int level, Slice begin, Slice end)
		{
			return state.Compactor.CompactAsync(level, begin, end);
		}

		public Snapshot CreateSnapshot()
		{
			return CreateSnapshotAsync().Result;
		}

		public async Task<Snapshot> CreateSnapshotAsync()
		{
			using (var locker = await state.Lock.LockAsync())
			{
				return await state.Snapshooter.CreateNew(this.state.VersionSet, locker);
			}
		}

		public void ReleaseSnapshot(Snapshot snapshot)
		{
			ReleaseSnapshotAsync(snapshot).Wait();
		}

		public async Task ReleaseSnapshotAsync(Snapshot snapshot)
		{
			using (var locker = await state.Lock.LockAsync())
			{
				await state.Snapshooter.Delete(snapshot, locker);
			}
		}
	}
}