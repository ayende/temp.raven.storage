namespace Raven.Storage.Impl
{
	using System;
	using System.Collections.Generic;
	using System.IO;

	using Raven.Storage.Data;
	using Raven.Storage.Impl.Streams;
	using Raven.Storage.Util;

	public class VersionEdit
	{
		private bool HasComparator
		{
			get
			{
				return !string.IsNullOrEmpty(Comparator);
			}
		}

		public string Comparator { get; private set; }

		public ulong? LogNumber { get; private set; }

		public ulong? PrevLogNumber { get; private set; }

		public ulong? NextFileNumber { get; private set; }

		public ulong? LastSequence { get; private set; }

		public IDictionary<int, IList<Slice>> CompactionPointers { get; private set; }

		public IDictionary<int, IList<ulong>> DeletedFiles { get; set; }

		public IDictionary<int, IList<FileMetadata>> NewFiles { get; set; }

		public VersionEdit()
		{
			Clear();
		}

		private void Clear()
		{
			Comparator = null;
			LogNumber = null;
			PrevLogNumber = null;
			LastSequence = null;
			NextFileNumber = null;

			CompactionPointers = new Dictionary<int, IList<Slice>>();
			DeletedFiles = new Dictionary<int, IList<ulong>>();
			NewFiles = new Dictionary<int, IList<FileMetadata>>();

			for (int level = 0; level < Config.NumberOfLevels; level++)
			{
				CompactionPointers.Add(level, new List<Slice>());
				DeletedFiles.Add(level, new List<ulong>());
				NewFiles.Add(level, new List<FileMetadata>());
			}
		}

		public void SetComparatorName(Slice name)
		{
			Comparator = name.ToString();
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
			NextFileNumber = number;
		}

		public void SetLastSequence(ulong sequence)
		{
			LastSequence = sequence;
		}

		public void SetCompactionPointer(int level, Slice key)
		{
			CompactionPointers[level].Add(key);
		}

		public void AddFile(int level, FileMetadata file)
		{
			NewFiles[level].Add(file);
		}

		public void EncodeTo(LogWriter writer)
		{
			var stream = new MemoryStream();

			if (HasComparator)
			{
				stream.Write7BitEncodedInt((int)Tag.Comparator);
				stream.WriteLengthPrefixedSlice(Comparator);
			}

			if (LogNumber.HasValue)
			{
				stream.Write7BitEncodedInt((int)Tag.LogNumber);
				stream.Write7BitEncodedLong(LogNumber.Value);
			}

			if (PrevLogNumber.HasValue)
			{
				stream.Write7BitEncodedInt((int)Tag.PrevLogNumber);
				stream.Write7BitEncodedLong(PrevLogNumber.Value);
			}

			if (NextFileNumber.HasValue)
			{
				stream.Write7BitEncodedInt((int)Tag.NextFileNumber);
				stream.Write7BitEncodedLong(NextFileNumber.Value);
			}

			if (LastSequence.HasValue)
			{
				stream.Write7BitEncodedInt((int)Tag.LastSequence);
				stream.Write7BitEncodedLong(LastSequence.Value);
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

			var data = stream.ToArray();
			writer.RecordStarted();
			writer.WriteAsync(data, 0, data.Length).Wait();
			writer.RecordCompletedAsync().Wait();
		}

		public static VersionEdit DecodeFrom(Stream stream)
		{
			var result = new VersionEdit();

			while (true)
			{
				Tag tag;
				try
				{
					tag = (Tag)stream.Read7BitEncodedInt();
				}
				catch (EndOfStreamException)
				{
					break;
				}
				
				int level;
				switch (tag)
				{
					case Tag.Comparator:
						var slice = stream.ReadLengthPrefixedSlice();
						result.SetComparatorName(slice);
						break;
					case Tag.LogNumber:
						result.SetLogNumber((ulong) stream.Read7BitEncodedLong());
						break;
					case Tag.PrevLogNumber:
						result.SetPrevLogNumber((ulong) stream.Read7BitEncodedLong());
						break;
					case Tag.NextFileNumber:
						result.SetNextFile((ulong) stream.Read7BitEncodedLong());
						break;
					case Tag.LastSequence:
						result.SetLastSequence((ulong) stream.Read7BitEncodedLong());
						break;
					case Tag.CompactPointer:
						level = stream.Read7BitEncodedInt();
						var compactionPointer = stream.ReadLengthPrefixedSlice();

						result.SetCompactionPointer(level, compactionPointer);
						break;
					case Tag.DeletedFile:
						level = stream.Read7BitEncodedInt();
						var fileNumber = (ulong) stream.Read7BitEncodedLong();

						result.DeleteFile(level, fileNumber);
						break;
					case Tag.NewFile:
						level = stream.Read7BitEncodedInt();
						var fileMetadata = new FileMetadata();
						fileMetadata.FileNumber = (ulong) stream.Read7BitEncodedLong();
						fileMetadata.FileSize = stream.Read7BitEncodedLong();
						fileMetadata.SmallestKey = stream.ReadLengthPrefixedSlice();
						fileMetadata.LargestKey = stream.ReadLengthPrefixedSlice();

						result.AddFile(level, fileMetadata);
						break;
				}
			}

			return result;
		}

		public void DeleteFile(int level, ulong fileNumber)
		{
			DeletedFiles[level].Add(fileNumber);
		}
	}
}