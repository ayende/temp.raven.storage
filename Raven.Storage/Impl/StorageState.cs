using System.Threading.Tasks;

namespace Raven.Storage.Impl
{
	public class StorageState
	{
		public Memtable.MemTable MemTable;
		public volatile Memtable.MemTable ImmutableMemTable;
		public volatile bool BackgroundCompactionScheduled;
		public volatile Task BackgroundTask = Task.FromResult<object>(null);
		public volatile bool ShuttingDown;

		public LogWriter LogWriter;

		public AsyncLock Lock;
		public VersionSet VersionSet;
		public StorageOptions Options;
		public FileSystem FileSystem;
		public string DatabaseName;
		public int LogFileNumber;
	}
}