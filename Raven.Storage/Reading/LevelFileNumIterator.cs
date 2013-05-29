namespace Raven.Storage.Reading
{
	using System.Collections.Generic;
	using System.Diagnostics;
	using System.IO;

	using Raven.Storage.Comparing;
	using Raven.Storage.Data;
	using Raven.Storage.Impl;

	public class LevelFileNumIterator : IIterator
	{
		private readonly InternalKeyComparator internalKeyComparator;

		private readonly IList<FileMetadata> files;

		private int index;

		public LevelFileNumIterator(InternalKeyComparator internalKeyComparator, IList<FileMetadata> files)
		{
			this.internalKeyComparator = internalKeyComparator;
			this.files = files;

			MarkAsInvalid();
		}

		public void Dispose()
		{
		}

		public bool IsValid
		{
			get
			{
				return index < files.Count;
			}
		}

		public void SeekToFirst()
		{
			index = 0;
		}

		public void SeekToLast()
		{
			index = files.Count == 0 ? 0 : files.Count - 1;
		}

		public void Seek(Slice target)
		{
			index = FindFile(target);
		}

		public void Next()
		{
			Debug.Assert(IsValid);

			index++;
		}

		public void Prev()
		{
			Debug.Assert(IsValid);

			if (index == 0)
			{
				MarkAsInvalid();
				return;
			}

			index--;
		}

		public Slice Key
		{
			get
			{
				return files[index].LargestKey;
			}
		}

		public Stream CreateValueStream()
		{
			throw new System.NotImplementedException();
		}

		private void MarkAsInvalid()
		{
			index = files.Count;
		}

		private int FindFile(Slice key)
		{
			var left = 0;
			var right = files.Count;

			while (left < right)
			{
				var mid = (left + right) / 2;
				var file = files[mid];

				if (internalKeyComparator.Compare(file.LargestKey, key) < 0)
				{
					// Key at "mid.largest" is < "target".  Therefore all
					// files at or before "mid" are uninteresting.
					left = mid + 1;
				}
				else
				{
					// Key at "mid.largest" is >= "target".  Therefore all files
					// after "mid" are uninteresting.
					right = mid;
				}
			}

			return right;
		}
	}
}