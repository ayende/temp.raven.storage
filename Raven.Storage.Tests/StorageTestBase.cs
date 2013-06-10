namespace Raven.Storage.Tests
{
	using System;
	using System.Collections.Generic;
	using System.IO;
	using System.Xml;

	using NLog.Config;

	public abstract class StorageTestBase : IDisposable
	{
		private readonly IList<Storage> storages;

		protected StorageTestBase()
		{
			ConfigureLogging();
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

				//ClearDatabaseDirectory(storage.Name);
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

		private void ConfigureLogging()
		{
			using (var stream = this.GetType().Assembly.GetManifestResourceStream("Raven.Storage.Tests.NLog.config"))
			using (var reader = XmlReader.Create(stream))
			{
				NLog.LogManager.Configuration = new XmlLoggingConfiguration(reader, "default-config");
			}
		}
	}
}