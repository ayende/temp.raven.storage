using System;
using System.IO;
using Raven.Storage.Data;

namespace Raven.Storage.Reading
{
	public interface IIterator : IDisposable
	{
		/// <summary>
		///  An iterator is either positioned at a key/value pair, or
		/// not valid.  This method returns true iff the iterator is valid.
		/// </summary>
		bool IsValid { get; }

		/// <summary>
		/// Position at the first key in the source.  The iterator is IsValid
		/// after this call iff the source is not empty.
		/// </summary>
		void SeekToFirst();

		/// <summary>
		/// Position at the last key in the source.  The iterator is
		/// IsValid after this call iff the source is not empty.
		/// </summary>
		void SeekToLast();

		/// <summary>
		/// Position at the first key in the source that at or past target
		/// The iterator is IsValid after this call iff the source contains
		/// an entry that comes at or past target.
		/// </summary>
		void Seek(Slice target);

		/// <summary>
		///  Moves to the next entry in the source.  After this call, IsValid is
		///  true iff the iterator was not positioned at the last entry in the source.
		///  REQUIRES: IsValid
		/// </summary>
		void Next();

		/// <summary>
		/// Moves to the previous entry in the source.  After this call, IsValid is
		/// true iff the iterator was not positioned at the first entry in source.
		/// REQUIRES: IsValid
		/// </summary>
		void Prev();

		/// <summary>
		///  Return the key for the current entry.  The underlying storage for
		/// the returned key is valid only until the next modification of
		/// the iterator.
		/// REQUIRES: IsValid
		/// </summary>
		Slice Key { get; }

		/// <summary>
		/// Return the value for the current entry. 
		/// REQUIRES: IsValid
		/// </summary>
		Stream CreateValueStream();
	}
}