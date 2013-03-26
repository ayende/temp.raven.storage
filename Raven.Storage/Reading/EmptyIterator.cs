using System;
using System.Collections.Generic;
using System.IO;
using Raven.Storage.Data;

namespace Raven.Storage.Reading
{
	public class EmptyIterator : IIterator
	{
		private readonly List<IDisposable> _disposables = new List<IDisposable>();
		public bool IsValid
		{
			get { return false; }
		}
		public void SeekToFirst()
		{
		}

		public void SeekToLast()
		{
		}

		public void Seek(Slice target)
		{
		}

		public void Next()
		{
		}

		public void Prev()
		{
		}

		public Slice Key { get { throw new NotSupportedException("Cannot get a key when iterator not valid"); } }

		public Stream CreateValueStream()
		{
			return null;
		}

		public void RegisterCleanup(IDisposable item)
		{
			_disposables.Add(item);
		}

		public void Dispose()
		{
			foreach (var disposable in _disposables)
			{
				disposable.Dispose();
			}
		}
	}
}