using System;
using System.Collections.Concurrent;
using System.IO;
using Raven.Storage.Memory;

namespace Raven.Storage.Impl
{
	public class InMemoryFileSystem : FileSystem
	{
		private readonly ConcurrentDictionary<string, byte[]> _files =
			new ConcurrentDictionary<string, byte[]>(StringComparer.InvariantCultureIgnoreCase);

		public InMemoryFileSystem(string databaseName) : base(databaseName)
		{
		}

		public override Stream NewWritable(string name)
		{
			return new MemoryStreamAddToFileSystemOnDispose(this, name);
		}

		public override void DeleteFile(string name)
		{
			byte[] value;
			_files.TryRemove(name, out value);
		}

		public override System.Collections.Generic.IEnumerable<string> GetFiles()
		{
			return _files.Keys;
		}

		public override void Lock()
		{
			// don't need to do anything here
		}

		public override Stream OpenForReading(string name)
		{
			byte[] value;
			if (_files.TryGetValue(name, out value) == false)
				throw new FileNotFoundException(name);
			return new MemoryStream(value, false);
		}

        public override bool Exists(string name)
        {
            return _files.ContainsKey(name);
        }

		public override void RenameFile(string source, string destination)
		{
			byte[] value;
			if (_files.TryRemove(source, out value) == false)
			{
				throw new FileNotFoundException(source);
			}
			_files.AddOrUpdate(destination, value, (s, bytes) => value);
		}

		public override IAccessor OpenMemoryMap(string name)
		{
			byte[] value;
			if (_files.TryGetValue(name, out value) == false)
			{
				throw new FileNotFoundException(name);
			}

			return new MemoryAccessor(value, value.Length);
		}

		public override string GetFullFileName(ulong num, string ext)
		{
			return string.Format("{0:000000}{1}", num, ext);
		}

		public class MemoryStreamAddToFileSystemOnDispose : MemoryStream
		{
			private readonly InMemoryFileSystem _fileSystem;
			private readonly string _name;
			private bool _disposed;
			public MemoryStreamAddToFileSystemOnDispose(InMemoryFileSystem fileSystem, string name)
			{
				_fileSystem = fileSystem;
				_name = name;
			}

			protected override void Dispose(bool disposing)
			{
				if (_disposed)
					return;
				_fileSystem._files.TryAdd(_name, ToArray());
				_disposed = true;
				base.Dispose(disposing);
			}
		}
	}
}