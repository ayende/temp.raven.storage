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
	}
}