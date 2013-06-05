namespace Raven.Storage.Impl
{
	using System.Threading.Tasks;

	using Raven.Storage.Data;

	public interface IStorageCommands
	{
		void Compact(int level, Slice begin, Slice end);

		Task CompactAsync(int level, Slice begin, Slice end);
	}
}