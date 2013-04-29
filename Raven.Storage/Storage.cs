using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using MS.Internal.Xml.XPath;
using Raven.Storage.Comparing;
using Raven.Storage.Memtable;

namespace Raven.Storage
{
	public class Storage : IDisposable
	{
		public string Name { get; private set; }
		public StorageOptions Options { get; private set; }

		private MemTable _memTable;

		private readonly SemaphoreSlim _writerLock = new SemaphoreSlim(0, 1);

		public Storage(string name, StorageOptions options)
		{
			Name = name;
			Options = options;
		}

		

		public void Dispose()
		{
			
		}
	}
}