using System.Collections;
using System.Collections.Generic;
using System.IO;
using Raven.Storage.Data;

namespace Raven.Streams
{
	public abstract class LowLevelStorage
	{
		public abstract Stream Create(string fileName);
		public abstract Stream CreateTemp();
		public abstract bool Exists(string fileName);
		public abstract Stream Read(string fileName);
		public abstract FileData FileData(string fileName);
		public abstract void Flush(Stream stream);
		public abstract void Delete(string fileName);
		public abstract IEnumerable<string> GetFileNamesInOrder();
	}
}