﻿namespace Raven.Storage.Impl
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
			using (await state.Lock.LockAsync().ConfigureAwait(false))
			{
				compactAsync = state.Compactor.Manual.CompactAsync(level, begin, end);
			}
			await compactAsync.ConfigureAwait(false); // we do the wait _outside_ the lock
		}

		public Task CompactRangeAsync(Slice begin, Slice end)
		{
			return state.Compactor.Manual.CompactRangeAsync(begin, end);
		}

		public Task CompactMemTableAsync()
		{
			return state.Compactor.Manual.CompactMemTableAsync();
		}

		public Task<StorageStatistics> GetStatisticsAsync()
		{
			return state.GetStorageStatisticsAsync();
		}

		public Snapshot CreateSnapshot()
		{
			return state.Snapshooter.CreateNewSnapshot(state.VersionSet);
		}

		public void ReleaseSnapshot(Snapshot snapshot)
		{
			state.Snapshooter.Delete(snapshot);
		}
	}
}