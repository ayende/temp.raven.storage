using System;
using System.IO.MemoryMappedFiles;
using Raven.Storage.Data;

namespace Raven.Storage.Reading
{
	public class Table
	{
		private readonly StorageOptions _storageOptions;
		private readonly MemoryMappedFile _mappedFile;
		private readonly long _size;

		public Table(	
			StorageOptions storageOptions,
			MemoryMappedFile mappedFile,
			long size)
		{
			_storageOptions = storageOptions;
			_mappedFile = mappedFile;
			_size = size;

			if (size < Footer.EncodedLength)
				throw new InvalidOperationException("File is too short to be an sstable");

			var footer = new Footer();
			using (var accessor = mappedFile.CreateViewAccessor(size - Footer.EncodedLength, Footer.EncodedLength))
			{
				footer.DecodeFrom(accessor);
			}
		}
	}
}