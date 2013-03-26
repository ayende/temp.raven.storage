using Raven.Storage.Data;

namespace Raven.Storage
{
	public interface IFilter
	{
		bool KeyMayMatch(long position, Slice key);
	}
}