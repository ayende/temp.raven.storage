namespace Raven.Storage.Util
{
	using System.Collections.Generic;

	using Raven.Storage.Comparing;
	using Raven.Storage.Data;
	using Raven.Storage.Impl;

	public static class FileExtensions
	{
		 public static bool TryFindFile(this IList<FileMetadata> files, Slice key, InternalKeyComparator internalKeyComparator, out int fileIndex)
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

			 fileIndex = right;

			 return fileIndex < files.Count;
		 }
	}
}