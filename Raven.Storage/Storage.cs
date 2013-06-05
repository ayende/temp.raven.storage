namespace Raven.Storage
{
	using System;

	using Raven.Storage.Impl;

	public class Storage : IDisposable
	{
		private readonly StorageState storageState;

		public Storage(string name, StorageOptions options)
		{
			this.storageState = new StorageState(name, options);
			Init();
		}

		public Storage(StorageState storageState)
		{
			this.storageState = storageState;
			Init();
		}

		private void Init()
		{
			//_storageState.Recover();
			this.storageState.CreateNewLog();
			this.Writer = new StorageWriter(this.storageState);
			this.Reader = new StorageReader(this.storageState);
		}

		public StorageWriter Writer { get; private set; }

		public StorageReader Reader { get; private set; }

		public void Dispose()
		{
			this.storageState.Dispose();
		}
	}
}