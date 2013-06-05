namespace Raven.Storage.Impl
{
	using Raven.Storage.Comparing;
	using Raven.Storage.Impl.Caching;

	public interface IStorageContext
	{
		AsyncLock Lock { get; }

		StorageOptions Options { get; }

		FileSystem FileSystem { get; }

		string DatabaseName { get; }

		TableCache TableCache { get; }

		InternalKeyComparator InternalKeyComparator { get; }
	}
}