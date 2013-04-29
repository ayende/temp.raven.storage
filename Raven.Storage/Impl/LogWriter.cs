using System;
using System.Collections.Generic;
using System.IO;

namespace Raven.Storage.Impl
{
	public class LogWriter : IDisposable
	{
		public LogWriter(Stream file)
		{
			throw new NotImplementedException();
		}

		public void AddRecord(IEnumerable<WriteBatch> list)
		{
			throw new System.NotImplementedException();
		}

		public void Dispose()
		{
			throw new System.NotImplementedException();
		}
	}
}