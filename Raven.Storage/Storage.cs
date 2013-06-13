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
			InitAsync().Wait();
		}

		public Storage(StorageState storageState)
		{
			this.storageState = storageState;
			InitAsync().Wait();
		}

		private async Task InitAsync()
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
				await storageState.Compactor.MaybeScheduleCompactionAsync(locker);
			}
		}

		public IStorageCommands Commands { get; private set; }

		public StorageWriter Writer { get; private set; }

		public StorageReader Reader { get; private set; }

		public void Dispose()
		{
			if (wasDisposed)
				return;

			this.storageState.Dispose();
			wasDisposed = true;
		}
	}
}