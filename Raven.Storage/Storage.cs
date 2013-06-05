namespace Raven.Storage
{
	using System;

	using Raven.Storage.Impl;

	public class Storage : IDisposable
	{
		private readonly StorageState storageState;

		public Storage(string name, StorageOptions options)
		{
			storageState = new StorageState(name, options);
			Init();
		}

		public Storage(StorageState storageState)
		{
			this.storageState = storageState;
			Init();
		}

		private void Init()
		{
			storageState.Recover();
			storageState.CreateNewLog();
			Writer = new StorageWriter(storageState);
			Reader = new StorageReader(storageState);
			Commands = new StorageCommands(storageState);
		}

		public IStorageCommands Commands { get; private set; }

		public StorageWriter Writer { get; private set; }

		public StorageReader Reader { get; private set; }

		public void Dispose()
		{
		this.storageState.Dispose();
		}
	}
}