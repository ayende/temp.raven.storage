using System.IO.MemoryMappedFiles;
using Raven.Storage.Data;

namespace Raven.Storage.Filtering
{
	/// <summary>
	/// A database can be configured with a custom FilterPolicy object.
	/// This object is responsible for creating a small filter from a set
	/// of keys.  These filters are stored in leveldb and are consulted
	/// automatically by the db to decide whether or not to read some
	/// information from disk. In many cases, a filter can cut down the
	/// number of disk seeks form a handful to a single disk seek per
	/// Get() call.
	/// 
	/// Most people will want to use the builtin bloom filter support.
	/// </summary>
	public interface IFilterPolicy
	{
		IFilterBuilder CreateBuilder();
		string Name { get; }
		IFilter CreateFilter(MemoryMappedViewAccessor accessor);
	}
}