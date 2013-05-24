namespace Raven.Storage.Impl
{
	using System;

	using Raven.Storage.Data;

	public class VersionEdit
	{
		private bool HasComparator
		{
			get
			{
				return !string.IsNullOrEmpty(comparator);
			}
		}

		private bool HasLogNumber
		{
			get
			{
				return logNumber > 0;
			}
		}

		private bool HasPrevLogNumber
		{
			get
			{
				return prevLogNumber > 0;
			}
		}

		private bool HasNextFileNumber
		{
			get
			{
				return nextFileNumber > 0;
			}
		}

		//private bool HasLastSequence
		//{
		//	get
		//	{
		//		return lastSequence != null;
		//	}
		//}

		private string comparator;

		private ulong logNumber;

		private ulong prevLogNumber;

		private ulong nextFileNumber;

		//private SequenceNumber lastSequence;

		public VersionEdit()
		{
			Clear();
		}

		private void Clear()
		{
			throw new NotImplementedException();
		}

		private void SetComparatorName(Slice name)
		{
			comparator = name.ToString();
		}

		public void SetLogNumber(ulong number)
		{
			logNumber = number;
		}

		public void SetPrevLogNumber(ulong number)
		{
			prevLogNumber = number;
		}

		private void SetNextFile(ulong number)
		{
			nextFileNumber = number;
		}

		//private void SetLastSequence(SequenceNumber sequence)
		//{
		//	lastSequence = sequence;
		//}

		//private void SetCompactorPointer(int level, InternalKey key)
		//{
		//	throw new NotImplementedException();
		//}

		//private void AddFile(int level, ulong file, ulong fileSize, InternalKey smallest, InternalKey largest)
		//{
		//	throw new NotImplementedException();
		//}

		private void DeleteFile(int level, ulong file)
		{
			throw new NotImplementedException();
		}

		private void EncodeTo(Slice dst)
		{
			throw new NotImplementedException();
		}

		Status DecodeFrom(Slice src)
		{
			throw new NotImplementedException();
		}
	}
}