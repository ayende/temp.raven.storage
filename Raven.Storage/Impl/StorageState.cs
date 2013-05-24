using System;
using System.IO;
using System.Threading.Tasks;
using Raven.Storage.Impl.Streams;

namespace Raven.Storage.Impl
{
	public class StorageState : IDisposable
	{
		public Memtable.MemTable MemTable;
		public volatile Memtable.MemTable ImmutableMemTable;
		public volatile bool BackgroundCompactionScheduled;
		public volatile Task BackgroundTask = Task.FromResult<object>(null);
		public volatile bool ShuttingDown;

		public LogWriterStream LogWriter;

		public AsyncLock Lock;
		public VersionSet VersionSet;
		public StorageOptions Options;
		public FileSystem FileSystem;
		public string DatabaseName;
		public ulong LogFileNumber { get; private set; }

		public void CreateNewLog()
		{
			var newFileNumber = VersionSet.NewFileNumber();
			try
			{
				var file = FileSystem.NewWritable(DatabaseName, newFileNumber, "log");
				LogWriter = new LogWriterStream(file);
				LogFileNumber = newFileNumber;
			}
			catch (Exception)
			{
				// Avoid chewing through file number space in a tight loop.
				VersionSet.ReuseFileNumber(newFileNumber);
				throw;
			}
		}

		public void Dispose()
		{
			if (LogWriter != null)
				LogWriter.Dispose();
			if (FileSystem != null)
				FileSystem.Dispose();
			if(MemTable != null)
				MemTable.Dispose();
			if(ImmutableMemTable != null)
				ImmutableMemTable.Dispose();
		}
	}
}