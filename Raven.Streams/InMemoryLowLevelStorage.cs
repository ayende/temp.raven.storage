using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using Raven.Storage.Data;
using Raven.Storage.Memory;
using Raven.Storage.Util;
using System.Linq;

namespace Raven.Streams
{
	public class InMemoryLowLevelStorage : LowLevelStorage
	{
		private readonly BufferPool _bufferPool;

		private readonly ConcurrentDictionary<string, Tuple<byte[], long>> _files =
			new ConcurrentDictionary<string, Tuple<byte[], long>>(StringComparer.InvariantCultureIgnoreCase);

		public InMemoryLowLevelStorage(BufferPool bufferPool)
		{
			_bufferPool = bufferPool;
		}

		public override Stream Create(string fileName)
		{
			return new InMemoryFileStream(_bufferPool, fileName, this);
		}

		public override Stream CreateTemp()
		{
			return new BufferPoolMemoryStream(_bufferPool);
		}

		public override bool Exists(string fileName)
		{
			return _files.ContainsKey(fileName);
		}

		public override Stream Read(string fileName)
		{
			Tuple<byte[], long> value;
			if (_files.TryGetValue(fileName, out value) == false)
				throw new FileNotFoundException(fileName);
			return new MemoryStream(value.Item1, 0, (int) value.Item2, false);
		}

		public override FileData FileData(string fileName)
		{
			Tuple<byte[], long> value;
			if (_files.TryGetValue(fileName, out value) == false)
				throw new FileNotFoundException(fileName);
			return new FileData(new MemoryAccessor(value.Item1, value.Item2), value.Item2);
		}

		public override void Flush(Stream stream)
		{
			
		}

		public override void Delete(string fileName)
		{
			Tuple<byte[], long> value;
			if (_files.TryRemove(fileName, out value))
				_bufferPool.Return(value.Item1);
		}

		public override IEnumerable<string> GetFileNamesInOrder()
		{
			return _files.Keys.OrderBy(x => x);
		}

		private class InMemoryFileStream : BufferPoolMemoryStream
		{
			private readonly string _name;
			private readonly InMemoryLowLevelStorage _storage;

			public InMemoryFileStream(BufferPool bufferPool, string name, InMemoryLowLevelStorage storage) : base(bufferPool)
			{
				_name = name;
				_storage = storage;
			}

			protected override void Dispose(bool disposing)
			{
				if(_storage._files.TryAdd(_name, Tuple.Create(_buffer, _length)) == false)
					throw new InvalidOperationException("Cannot add file: " + _name + ", already exists");
			}
		}
	}
}