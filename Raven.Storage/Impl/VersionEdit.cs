namespace Raven.Storage.Impl
{
	using System;
	using System.Collections.Generic;
	using System.IO;

	using Raven.Storage.Data;
	using Raven.Storage.Util;

	public class VersionEdit
	{
		private bool HasComparator
		{
			get
			{
				return !string.IsNullOrEmpty(comparator);
			}
		}

		public bool HasLogNumber
		{
			get
			{
				return LogNumber > 0;
			}
		}

		public bool HasPrevLogNumber
		{
			get
			{
				return PrevLogNumber > 0;
			}
		}

		private bool HasNextFileNumber
		{
			get
			{
				return nextFileNumber > 0;
			}
		}

		private bool HasLastSequence
		{
			get
			{
				return lastSequence > 0;
			}
		}

		private string comparator;

		public ulong LogNumber { get; private set; }

		public ulong PrevLogNumber { get; private set; }

		private ulong nextFileNumber;

		private ulong lastSequence;

		public IDictionary<int, IList<Slice>> CompactionPointers { get; private set; }

		public IDictionary<int, IList<ulong>> DeletedFiles { get; set; }

		public IDictionary<int, IList<FileMetadata>> NewFiles { get; set; }

		public VersionEdit()
		{
			Clear();
		}

		private void Clear()
		{
			comparator = null;
			LogNumber = 0;
			PrevLogNumber = 0;
			lastSequence = 0;
			nextFileNumber = 0;

			this.CompactionPointers = new Dictionary<int, IList<Slice>>();
			this.DeletedFiles = new Dictionary<int, IList<ulong>>();
			this.NewFiles = new Dictionary<int, IList<FileMetadata>>();

			for (int level = 0; level < Config.NumberOfLevels; level++)
			{
				this.CompactionPointers.Add(level, new List<Slice>());
				this.DeletedFiles.Add(level, new List<ulong>());
				this.NewFiles.Add(level, new List<FileMetadata>());
			}
		}

		public void SetComparatorName(Slice name)
		{
			comparator = name.ToString();
		}

		public void SetLogNumber(ulong number)
		{
			LogNumber = number;
		}

		public void SetPrevLogNumber(ulong number)
		{
			PrevLogNumber = number;
		}

		public void SetNextFile(ulong number)
		{
			nextFileNumber = number;
		}

		public void SetLastSequence(ulong sequence)
		{
			lastSequence = sequence;
		}

		public void SetCompactionPointer(int level, Slice key)
		{
			throw new NotImplementedException();
		}

		public void AddFile(int level, FileMetadata file)
		{
			throw new NotImplementedException();
		}

		public void EncodeTo(Stream stream)
		{
			if (HasComparator)
			{
				stream.Write7BitEncodedInt((int)Tag.Comparator);
				stream.WriteLengthPrefixedSlice(comparator);
			}

			if (HasLogNumber)
			{
				stream.Write7BitEncodedInt((int)Tag.LogNumber);
				stream.Write7BitEncodedLong(LogNumber);
			}

			if (HasPrevLogNumber)
			{
				stream.Write7BitEncodedInt((int)Tag.PrevLogNumber);
				stream.Write7BitEncodedLong(PrevLogNumber);
			}

			if (HasNextFileNumber)
			{
				stream.Write7BitEncodedInt((int)Tag.NextFileNumber);
				stream.Write7BitEncodedLong(nextFileNumber);
			}

			if (HasLastSequence)
			{
				stream.Write7BitEncodedInt((int)Tag.LastSequence);
				stream.Write7BitEncodedLong(lastSequence);
			}

			for (int level = 0; level < Config.NumberOfLevels; level++)
			{
				foreach (var compactionPointer in CompactionPointers[level])
				{
					stream.Write7BitEncodedInt((int)Tag.CompactPointer);
					stream.Write7BitEncodedInt(level);
					stream.WriteLengthPrefixedSlice(compactionPointer);
				}

				foreach (var fileNumber in DeletedFiles[level])
				{
					stream.Write7BitEncodedInt((int)Tag.DeletedFile);
					stream.Write7BitEncodedInt(level);
					stream.Write7BitEncodedLong(fileNumber);
				}

				foreach (var fileMetadata in NewFiles[level])
				{
					stream.Write7BitEncodedInt((int)Tag.NewFile);
					stream.Write7BitEncodedInt(level);
					stream.Write7BitEncodedLong(fileMetadata.FileNumber);
					stream.Write7BitEncodedLong(fileMetadata.FileSize);
					stream.WriteLengthPrefixedSlice(fileMetadata.SmallestKey);
					stream.WriteLengthPrefixedSlice(fileMetadata.LargestKey);
				}
			}
		}

		public void DeleteFile(int level, ulong fileNumber)
		{
			DeletedFiles[level].Add(fileNumber);
		}
	}
}