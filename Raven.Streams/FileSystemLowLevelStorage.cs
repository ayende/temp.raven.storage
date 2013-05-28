using System;
using System.Collections.Generic;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Threading.Tasks;
using Raven.Storage.Data;
using Raven.Storage.Memory;

namespace Raven.Streams
{
	public class FileSystemLowLevelStorage : LowLevelStorage
	{
		private readonly string _path;

		public FileSystemLowLevelStorage(string path)
		{
			_path = Path.GetFullPath(path);
			if (Directory.Exists(_path) == false)
				Directory.CreateDirectory(_path);
		}

		public override Stream Create(string fileName)
		{
			return File.Create(Path.Combine(_path, fileName));
		}


		public override Stream CreateTemp()
		{
			return File.Create(Path.GetTempFileName(), 4096, FileOptions.DeleteOnClose);

		}

		public override bool Exists(string fileName)
		{
			return File.Exists(Path.Combine(_path, fileName));
		}

		public override Stream Read(string fileName)
		{
			return File.OpenRead(Path.Combine(_path, fileName));
		}

		public override FileData FileData(string fileName)
		{
			var fileInfo = new FileInfo(Path.Combine(_path, fileName));
			return new FileData(new MemoryMappedFileAccessor(MemoryMappedFile.OpenExisting(fileInfo.FullName)), fileInfo.Length);
		}

		public override void Flush(Stream stream)
		{
			((FileStream) stream).Flush(true);
		}

		public override void Delete(string fileName)
		{
			var file = Path.Combine(_path, fileName);
			if (File.Exists(file))
				File.Delete(file);
		}

		public override IEnumerable<string> GetFileNamesInOrder()
		{
			return Directory.EnumerateFiles(_path);
		}
	}
}