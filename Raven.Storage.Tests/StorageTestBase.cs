using Raven.Storage.Impl;
using Raven.Storage.Util;

namespace Raven.Storage.Tests
{
	using System;
	using System.Collections.Generic;
	using System.IO;
	using System.Xml;

	using NLog.Config;
	using System.Threading.Tasks;

	public abstract class StorageTestBase : IDisposable
	{
		private readonly IList<Storage> storages;

		protected StorageTestBase()
		{
			ConfigureLogging();
			storages = new List<Storage>();
		}

		public async Task<Storage> NewStorageAsync(StorageOptions storageOptions = null, FileSystem fileSystem = null)
		{
			if (storageOptions == null)
				storageOptions = new StorageOptions();

			string name = string.Format("TestStorage-{0}-{1}", DateTime.Now.ToString("yyyy-MM-dd,HH-mm-ss"), Guid.NewGuid());
			var storage = new Storage(new StorageState(name, storageOptions)
				{
					FileSystem = fileSystem ?? new InMemoryFileSystem(name)
				});
			await storage.InitAsync();
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
					if (Directory.Exists(directory))
						Directory.Delete(directory, true);
					return;
				}
				catch (IOException)
				{
					if (isRetry)
						throw;
				}
				catch (UnauthorizedAccessException)
				{
					if (isRetry)
						throw;
				}

				TrackResourceUsage.Pending();

				GC.Collect();
				GC.WaitForPendingFinalizers();
				isRetry = true;
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