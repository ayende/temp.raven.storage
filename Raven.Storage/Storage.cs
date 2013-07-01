using System.Threading.Tasks;

namespace Raven.Storage
{
	using System;

	using Raven.Storage.Impl;

	public class Storage : IDisposable
	{
		private readonly StorageState _storageState;

		private bool _wasDisposed = false;

		public string Name
		{
			get
			{
				return _storageState.DatabaseName;
			}
		}

		public Storage(string name, StorageOptions options)
		{
			_storageState = new StorageState(name, options);
		}

		public Storage(StorageState storageState)
		{
			_storageState = storageState;
		}

		public StorageState StorageState
		{
			get { return _storageState; }
		}

		public async Task InitAsync()
		{
			var edit = _storageState.Recover();
			
			_storageState.CreateNewLog();
			edit.SetComparatorName(_storageState.Options.Comparator.Name);
			edit.SetLogNumber(_storageState.LogFileNumber);

			Writer = new StorageWriter(_storageState);
			Reader = new StorageReader(_storageState);
			Commands = new StorageCommands(_storageState);
			using (var locker = await _storageState.Lock.LockAsync().ConfigureAwait(false))
			{
				await _storageState.LogAndApplyAsync(edit, locker).ConfigureAwait(false);
				_storageState.Compactor.DeleteObsoleteFiles();
				_storageState.Compactor.MaybeScheduleCompaction(locker);
			}
		}

		public IStorageCommands Commands { get; private set; }

		public StorageWriter Writer { get; private set; }

		public StorageReader Reader { get; private set; }

		public void Dispose()
		{
			if (_wasDisposed)
				return;

			_storageState.Dispose();
			_wasDisposed = true;
		}
	}
}