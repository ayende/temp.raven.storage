namespace Raven.Storage.Reading
{
	using System.Collections.Generic;
	using System.Diagnostics;
	using System.IO;

	using Raven.Storage.Comparing;
	using Raven.Storage.Data;
	using Raven.Storage.Impl;
	using Raven.Storage.Util;

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
			files.TryFindFile(target, internalKeyComparator, out index);
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
				return files[index].LargestKey.TheInternalKey;
			}
		}

		public Stream CreateValueStream()
		{
			var stream = new MemoryStream();

			new BlockHandle
				{
					Position = (long) files[index].FileNumber,
					Count = files[index].FileSize
				}.EncodeTo(stream);
			stream.Position = 0;
			return stream;
		}

		private void MarkAsInvalid()
		{
			index = files.Count;
		}
	}
}