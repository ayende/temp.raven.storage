using System.Text;

namespace Raven.Storage.Impl
{
    using System.Collections.Generic;
    using System.IO;
    using System.Threading.Tasks;

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

        public IDictionary<int, IList<InternalKey>> CompactionPointers { get; private set; }

        public IDictionary<int, IList<ulong>> DeletedFiles { get; set; }

        public IDictionary<int, IList<FileMetadata>> NewFiles { get; set; }

        public string DebugInfo
        {
            get
            {
                var sb = new StringBuilder();

                sb.Append("Comparator: ").AppendLine(Comparator);
                sb.Append("LogNumber: ").Append(LogNumber).AppendLine();
                sb.Append("PrevLogNumber: ").Append(PrevLogNumber).AppendLine();
                sb.Append("NextFileNumber: ").Append(NextFileNumber).AppendLine();
                sb.Append("LastSequence: ").Append(LastSequence).AppendLine();

                sb.AppendLine("NewFiles:");
                foreach (var newFile in NewFiles)
                {
                    if (newFile.Value.Count == 0)
                        continue;
                    sb.Append('\t').Append(newFile.Key).AppendLine();
                    foreach (var fileMetadata in newFile.Value)
                    {
                        sb.Append("\t\t").Append(fileMetadata).AppendLine();
                    }
                }
                sb.AppendLine("DeletedFiles:");
                foreach (var deletedFile in DeletedFiles)
                {
                    if (deletedFile.Value.Count == 0)
                        continue;
                    sb.Append('\t').Append(deletedFile.Key).Append(' ');
                    foreach (var u in deletedFile.Value)
                    {
                        sb.Append(u).Append(' ');
                    }
                    sb.AppendLine();
                }

                sb.AppendLine("CompactionPointers:");
                foreach (var cp in CompactionPointers)
                {
                    if(cp.Value.Count == 0)
                        continue;
                    sb.Append('\t').Append(cp.Key).Append(':');
                    foreach (var key in cp.Value)
                    {
                        sb.Append("\t\t").Append(key).AppendLine();
                    }
                    sb.AppendLine();
                }

                return sb.ToString();
            }
        }

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

            CompactionPointers = new Dictionary<int, IList<InternalKey>>();
            DeletedFiles = new Dictionary<int, IList<ulong>>();
            NewFiles = new Dictionary<int, IList<FileMetadata>>();

            for (int level = 0; level < Config.NumberOfLevels; level++)
            {
                CompactionPointers.Add(level, new List<InternalKey>());
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

        public void SetCompactionPointer(int level, InternalKey key)
        {
            CompactionPointers[level].Add(key);
        }

        public void AddFile(int level, FileMetadata file)
        {
            NewFiles[level].Add(file);
        }

        public void EncodeTo(LogWriter writer)
        {
            writer.RecordStarted();

            if (HasComparator)
            {
	            writer.Write7BitEncodedInt((int)Tag.Comparator);
	            writer.WriteLengthPrefixedSlice(Comparator);
            }

            if (LogNumber.HasValue)
            {
	            writer.Write7BitEncodedInt((int)Tag.LogNumber);
				writer.Write7BitEncodedLong((long)LogNumber.Value);
            }

            if (PrevLogNumber.HasValue)
            {
				writer.Write7BitEncodedInt((int)Tag.PrevLogNumber);
				writer.Write7BitEncodedLong((long)PrevLogNumber.Value);
            }

            if (NextFileNumber.HasValue)
            {
				writer.Write7BitEncodedInt((int)Tag.NextFileNumber);
				writer.Write7BitEncodedLong((long)NextFileNumber.Value);
            }

            if (LastSequence.HasValue)
            {
				writer.Write7BitEncodedInt((int)Tag.LastSequence);
				writer.Write7BitEncodedLong((long)LastSequence.Value);
            }

            for (int level = 0; level < Config.NumberOfLevels; level++)
            {
                foreach (var compactionPointer in CompactionPointers[level])
                {
					writer.Write7BitEncodedInt((int)Tag.CompactPointer);
					writer.Write7BitEncodedInt(level);
					writer.WriteLengthPrefixedInternalKey(compactionPointer);
                }

                foreach (var fileNumber in DeletedFiles[level])
                {
					writer.Write7BitEncodedInt((int)Tag.DeletedFile);
					writer.Write7BitEncodedInt(level);
					writer.Write7BitEncodedLong((long)fileNumber);
                }

                foreach (var fileMetadata in NewFiles[level])
                {
					writer.Write7BitEncodedInt((int)Tag.NewFile);
					writer.Write7BitEncodedInt(level);
					writer.Write7BitEncodedLong((long)fileMetadata.FileNumber);
					writer.Write7BitEncodedLong(fileMetadata.FileSize);
					writer.WriteLengthPrefixedInternalKey(fileMetadata.SmallestKey);
					writer.WriteLengthPrefixedInternalKey(fileMetadata.LargestKey);
                }
            }

	        writer.RecordCompleted();
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
                        result.SetLogNumber((ulong)stream.Read7BitEncodedLong());
                        break;
                    case Tag.PrevLogNumber:
                        result.SetPrevLogNumber((ulong)stream.Read7BitEncodedLong());
                        break;
                    case Tag.NextFileNumber:
                        result.SetNextFile((ulong)stream.Read7BitEncodedLong());
                        break;
                    case Tag.LastSequence:
                        result.SetLastSequence((ulong)stream.Read7BitEncodedLong());
                        break;
                    case Tag.CompactPointer:
                        level = stream.Read7BitEncodedInt();
                        var compactionPointer = stream.ReadLengthPrefixedInternalKey();

                        result.SetCompactionPointer(level, compactionPointer);
                        break;
                    case Tag.DeletedFile:
                        level = stream.Read7BitEncodedInt();
                        var fileNumber = (ulong)stream.Read7BitEncodedLong();

                        result.DeleteFile(level, fileNumber);
                        break;
                    case Tag.NewFile:
                        level = stream.Read7BitEncodedInt();
                        var fileMetadata = new FileMetadata
                                               {
                                                   FileNumber = (ulong)stream.Read7BitEncodedLong(),
                                                   FileSize = stream.Read7BitEncodedLong(),
                                                   SmallestKey = stream.ReadLengthPrefixedInternalKey(),
                                                   LargestKey = stream.ReadLengthPrefixedInternalKey()
                                               };

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