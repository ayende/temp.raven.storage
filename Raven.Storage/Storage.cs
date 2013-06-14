using System.Threading.Tasks;

namespace Raven.Storage
{
	using System;

	using Raven.Storage.Impl;

	public class Storage : IDisposable
	{
		private readonly StorageState storageState;

		private bool wasDisposed = false;

		public string Name
		{
			get
			{
				return storageState.DatabaseName;
			}
		}

		public Storage(string name, StorageOptions options)
		{
			storageState = new StorageState(name, options);
		}

		public Storage(StorageState storageState)
		{
			this.storageState = storageState;
		}

		public async Task InitAsync()
		{
			var edit = await storageState.RecoverAsync();
			
			storageState.CreateNewLog();
			edit.SetComparatorName(storageState.Options.Comparator.Name);
			edit.SetLogNumber(storageState.LogFileNumber);

			Writer = new StorageWriter(storageState);
			Reader = new StorageReader(storageState);
			Commands = new StorageCommands(storageState);
			using (var locker = await storageState.Lock.LockAsync())
			{
				await storageState.LogAndApplyAsync(edit, locker);
				storageState.Compactor.DeleteObsoleteFiles();
				storageState.Compactor.MaybeScheduleCompaction(locker);
			}
		}

		public IStorageCommands Commands { get; private set; }

		public StorageWriter Writer { get; private set; }

		public StorageReader Reader { get; private set; }

		public void Dispose()
		{
			if (wasDisposed)
				return;

			storageState.Dispose();
			wasDisposed = true;
		}
	}
}