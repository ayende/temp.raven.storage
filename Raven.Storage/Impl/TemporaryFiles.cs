using System;
using System.Collections.Generic;
using System.IO;

namespace Raven.Storage.Impl
{
	public class TemporaryFiles : IDisposable
	{
		private readonly FileSystem _fileSystem;
		private readonly ulong _fileNumber;
		private readonly List<string> _temps = new List<string>();

		public TemporaryFiles(FileSystem fileSystem, ulong fileNumber)
		{
			_fileSystem = fileSystem;
			_fileNumber = fileNumber;
		}


		public void Dispose()
		{
			foreach (var temp in _temps)
			{
				if (_fileSystem.Exists(temp))
					_fileSystem.DeleteFile(temp);
			}
		}

		public Stream Create()
		{
			var tempFileName = _fileSystem.GetTempFileName(_fileNumber);
			_temps.Add(tempFileName);
			return _fileSystem.NewWritable(tempFileName);
		}
	}
}