using Raven.Storage.Data;

namespace Raven.Storage.Filtering
{
	public interface IFilter
	{
		bool KeyMayMatch(long position, Slice key);
	}
}