using System.Collections.Generic;
using Raven.Storage.Data;

namespace Raven.Storage.Reading
{
	public static class IteratorExtensions
	{
		 public static bool WithPrefix(this IIterator iterator, Slice prefix)
		 {
			 return iterator.IsValid && iterator.Key.StartsWith(prefix);
		 }
	}
}