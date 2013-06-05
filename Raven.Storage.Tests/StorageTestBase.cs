namespace Raven.Storage.Tests
{
	using System;
	using System.Collections.Generic;
	using System.IO;

	public abstract class StorageTestBase : IDisposable
	{
		private readonly IList<Storage> storages;

		protected StorageTestBase()
		{
			storages = new List<Storage>();
		}

		public Storage NewStorage(StorageOptions storageOptions = null)
		{
			if (storageOptions == null)
				storageOptions = new StorageOptions();

			var storage = new Storage(string.Format("TestStorage-{0}", DateTime.Now.ToString("yyyy-MM-dd,HH-mm-ss")), storageOptions);
			storages.Add(storage);

			return storage;
		}

		public void Dispose()
		{
			foreach (var storage in storages)
			{
				try
				{
					storage.Dispose();
				}
				catch
				{
				}

				ClearDatabaseDirectory(storage.Name);
			}
		}

		protected void ClearDatabaseDirectory(string directory)
		{
			bool isRetry = false;

			while (true)
			{
				try
				{
					Directory.Delete(directory, true);
					break;
				}
				catch (IOException)
				{
					if (isRetry)
						throw;

					GC.Collect();
					GC.WaitForPendingFinalizers();
					isRetry = true;
				}
			}
		}
	}
}