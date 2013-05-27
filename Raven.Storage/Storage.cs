using System;
using Raven.Storage.Impl;
using Raven.Storage.Memtable;

namespace Raven.Storage
{
	public class Storage : IDisposable
	{
		private StorageWriter _storageWriter;
		private readonly StorageState _storageState;

		public Storage(string name, StorageOptions options)
		{
			_storageState = new StorageState
				{
					Options = options,
					MemTable = new MemTable(options),
					DatabaseName = name,
					Lock = new AsyncLock(),
					FileSystem = new FileSystem(),
					VersionSet = new VersionSet()
				};
			Init();
		}

		public Storage(StorageState storageState)
		{
			_storageState = storageState;
			Init();
		}

		private void Init()
		{
			//_storageState.Recover();
			_storageState.CreateNewLog();
			_storageWriter = new StorageWriter(_storageState);
		}

		public StorageWriter Writer {get { return _storageWriter; }}
	
		public void Dispose()
		{
			_storageState.Dispose();
		}
	}
}