namespace Raven.Storage.Impl
{
	using System;
	using System.Collections.Generic;
	using System.Diagnostics;
	using System.IO;
	using System.Linq;

	using Raven.Storage.Building;
	using Raven.Storage.Comparing;
	using Raven.Storage.Data;
	using Raven.Storage.Exceptions;
	using Raven.Storage.Impl.Compactions;
	using Raven.Storage.Impl.Streams;
	using Raven.Storage.Reading;
	using Raven.Temp.Logging;

	public class VersionSet
	{
		private readonly ILog log = LogManager.GetCurrentClassLogger();

		private ulong lastSequence;

		private Version current;

		public Slice[] CompactionPointers { get; private set; }

		private readonly IStorageContext storageContext;

		public VersionSet(IStorageContext storageContext)
		{
			this.storageContext = storageContext;

			NextFileNumber = 2;
			LogNumber = 0;
			ManifestFileNumber = 0;

			CompactionPointers = new Slice[Config.NumberOfLevels];

			AppendVersion(new Version(storageContext, this));
		}

		/// <summary>
		/// Return the last sequence number.
		/// </summary>
		public ulong LastSequence
		{
			get { return lastSequence; }
			set
			{
				Debug.Assert(value >= lastSequence);
				lastSequence = value;
			}
		}

		public Version Current
		{
			get
			{
				return current;
			}
		}

		/// <summary>
		/// Return the log file number for the log file that is currently
		/// being compacted, or zero if there is no such log file.
		/// </summary>
		public ulong PrevLogNumber { get; private set; }

		public ulong LogNumber { get; private set; }

		public bool NeedsCompaction
		{
			get
			{
				var v = current;
				return v.CompactionScore >= 1 || v.FileToCompact != null;
			}
		}

		public ulong ManifestFileNumber { get; private set; }

		public ulong NextFileNumber { get; private set; }

		public int GetNumberOfFilesAtLevel(int level)
		{
			return current.Files[level].Count;
		}

		public ulong NewFileNumber()
		{
			return NextFileNumber++;
		}

		public void ReuseFileNumber(ulong number)
		{
			if (NextFileNumber == number + 1)
			{
				NextFileNumber = number;
			}
		}

		public IList<ulong> GetLiveFiles()
		{
			Debug.Assert(current != null);

			var result = new List<ulong>();

			for (int level = 0; level < Config.NumberOfLevels; level++)
			{
				result.AddRange(current.Files[level].Select(file => file.FileNumber));
			}

			return result;
		}

		public void SetLogNumber(ulong number)
		{
			LogNumber = number;
		}

		public void SetPrevLogNumber(ulong number)
		{
			PrevLogNumber = number;
		}

		public void AppendVersion(Version version)
		{
			current = version;
		}

		public Compaction PickCompaction()
		{
			int level;
			Compaction compaction;

			bool sizeCompaction = Current.CompactionScore >= 1;
			bool seekCompaction = Current.FileToCompact != null;

			// We prefer compactions triggered by too much data in a level over
			// the compactions triggered by seeks.
			if (sizeCompaction)
			{
				level = Current.CompactionLevel;
				Debug.Assert(level >= 0);
				Debug.Assert(level + 1 < Config.NumberOfLevels);

				compaction = new Compaction(storageContext, level, Current);

				for (var i = 0; i < Current.Files[level].Count; i++)
				{
					var file = Current.Files[level][i];
					if (CompactionPointers[level].IsEmpty()
						|| storageContext.InternalKeyComparator.Compare(file.LargestKey.TheInternalKey, CompactionPointers[level]) > 0)
					{
						compaction.Inputs[0].Add(file);
						break;
					}
				}

				if (compaction.Inputs[0].Count == 0)
				{
					// Wrap-around to the beginning of the key space
					compaction.Inputs[0].Add(Current.Files[level][0]);
				}
			}
			else if (seekCompaction)
			{
				level = Current.FileToCompactLevel;
				compaction = new Compaction(storageContext, level, Current);
				compaction.Inputs[0].Add(Current.FileToCompact);
			}
			else
			{
				return null;
			}

			// Files in level 0 may overlap each other, so pick up all overlapping ones
			if (level == 0)
			{
				InternalKey smallestKey, largestKey;
				GetRange(compaction.Inputs[0], out smallestKey, out largestKey);

				// Note that the next call will discard the file we placed in
				// c->inputs_[0] earlier and replace it with an overlapping set
				// which will include the picked file.
				compaction.Inputs[0] = Current.GetOverlappingInputs(0, smallestKey, largestKey);
				Debug.Assert(compaction.Inputs[0].Count > 0);
			}

			SetupOtherInputs(compaction);

			return compaction;
		}

		public Compaction CompactRange(int level, InternalKey begin, InternalKey end)
		{
			var inputs = Current.GetOverlappingInputs(level, begin, end);
			if (inputs.Count == 0)
				return null;

			var compaction = new Compaction(storageContext, level, Current);
			compaction.Inputs[0] = inputs;

			SetupOtherInputs(compaction);

			return compaction;
		}

		private void SetupOtherInputs(Compaction compaction)
		{
			var level = compaction.Level;
			InternalKey smallestKey, largestKey;

			GetRange(compaction.Inputs[0], out smallestKey, out largestKey);

			compaction.Inputs[1] = Current.GetOverlappingInputs(level + 1, smallestKey, largestKey);

			InternalKey allStart, allLimit;
			GetRange2(compaction.Inputs[0], compaction.Inputs[1], out allStart, out allLimit);

			if (compaction.Inputs[1].Count > 0)
			{
				var expanded0 = Current.GetOverlappingInputs(level, allStart, allLimit);
				var inputs0Size = compaction.Inputs[0].Sum(x => x.FileSize);
				var inputs1Size = compaction.Inputs[1].Sum(x => x.FileSize);
				var expanded0Size = expanded0.Sum(x => x.FileSize);

				if (expanded0.Count > compaction.Inputs[0].Count
					&& inputs1Size + expanded0Size < Config.ExpandedCompactionByteSizeLimit)
				{
					InternalKey newStart, newLimit;
					GetRange(expanded0, out newStart, out newLimit);
					var expanded1 = Current.GetOverlappingInputs(level + 1, newStart, newLimit);

					if (expanded1.Count == compaction.Inputs[1].Count)
					{
						log.Info("Expanding@{0} {1}+{2} ({3}+{4} bytes) to {5}+{6} ({7}+{8} bytes).", level, compaction.Inputs[0].Count, compaction.Inputs[1].Count, inputs0Size, inputs1Size, expanded0.Count, expanded1.Count, expanded0Size, inputs1Size);

						largestKey = newLimit;

						compaction.Inputs[0] = expanded0;
						compaction.Inputs[1] = expanded1;

						GetRange2(compaction.Inputs[0], compaction.Inputs[1], out allStart, out allLimit);
					}
				}
			}

			if (level + 2 < Config.NumberOfLevels)
			{
				compaction.Grandparents = Current.GetOverlappingInputs(level + 2, allStart, allLimit);
			}

			// Update the place where we will do the next compaction for this level.
			// We update this immediately instead of waiting for the VersionEdit
			// to be applied so that if the compaction fails, we will try a different
			// key range next time.
			CompactionPointers[level] = largestKey.TheInternalKey;
			compaction.Edit.SetCompactionPointer(level, largestKey);
		}

		/// <summary>
		/// Stores the minimal range that covers all entries in inputs in
		/// *smallest, *largest.
		/// REQUIRES: inputs is not empty
		/// </summary>
		/// <param name="inputs"></param>
		/// <param name="smallestKey"></param>
		/// <param name="largestKey"></param>
		private void GetRange(IReadOnlyList<FileMetadata> inputs, out InternalKey smallestKey, out InternalKey largestKey)
		{
			Debug.Assert(inputs.Count > 0);

			smallestKey = new InternalKey();
			largestKey = new InternalKey();

			for (var i = 0; i < inputs.Count; i++)
			{
				var file = inputs[i];
				if (i == 0)
				{
					smallestKey = file.SmallestKey;
					largestKey = file.LargestKey;
				}
				else
				{
					if (storageContext.InternalKeyComparator.Compare(file.SmallestKey, smallestKey) < 0)
					{
						smallestKey = file.SmallestKey;
					}

					if (storageContext.InternalKeyComparator.Compare(file.LargestKey, largestKey) > 0)
					{
						largestKey = file.LargestKey;
					}
				}
			}
		}

		/// <summary>
		/// Stores the minimal range that covers all entries in inputs1 and inputs2
		/// in *smallest, *largest.
		/// REQUIRES: inputs is not empty
		/// </summary>
		/// <param name="inputs1"></param>
		/// <param name="inputs2"></param>
		/// <param name="smallestKey"></param>
		/// <param name="largestKey"></param>
		private void GetRange2(IEnumerable<FileMetadata> inputs1, IEnumerable<FileMetadata> inputs2, out InternalKey smallestKey, out InternalKey largestKey)
		{
			var all = new List<FileMetadata>(inputs1);
			all.AddRange(inputs2);

			GetRange(all, out smallestKey, out largestKey);
		}

		public IIterator MakeInputIterator(Compaction compaction)
		{
			var readOptions = new ReadOptions
								  {
									  VerifyChecksums = storageContext.Options.ParanoidChecks,
									  FillCache = false
								  };

			// Level-0 files have to be merged together.  For other levels,
			// we will make a concatenating iterator per level.
			// TODO(opt): use concatenating iterator for level-0 if there is no overlap
			var list = new List<IIterator>();
			for (int which = 0; which < 2; which++)
			{
				if (compaction.Inputs[which].Count != 0)
				{
					if (compaction.Level + which == 0)
					{
						var files = new List<FileMetadata>(compaction.Inputs[which]);
						list.AddRange(files.Select(file => storageContext.TableCache.NewIterator(readOptions, file.FileNumber, file.FileSize)));
					}
					else
					{
						// Create concatenating iterator for the files from this level
						list.Add(new TwoLevelIterator(
							         new LevelFileNumIterator(storageContext.InternalKeyComparator, compaction.Inputs[which]),
							         GetFileIterator, readOptions));
					}
				}
			}

			return NewMergingIterator(storageContext.InternalKeyComparator, list);
		}

		private IIterator GetFileIterator(ReadOptions readOptions, BlockHandle handle)
		{
			var fileNumber = (ulong)handle.Position;
			var fileSize = handle.Count;

			return storageContext.TableCache.NewIterator(readOptions, fileNumber, fileSize);
		}

		private IIterator NewMergingIterator(InternalKeyComparator comparator, IList<IIterator> list)
		{
			if (list.Count == 0)
				return new EmptyIterator();

			return list.Count == 1 ? list.First() : new MergingIterator(comparator, list);
		}

		public void Recover()
		{
			string currentManifest;

			using (var currentFile = storageContext.FileSystem.OpenForReading(storageContext.FileSystem.GetCurrentFileName()))
			using (var reader = new StreamReader(currentFile))
			{
				currentManifest = reader.ReadToEnd();
			}

			if (string.IsNullOrEmpty(currentManifest))
			{
				throw new FormatException("CURRENT file should not be empty.");
			}

            log.Info("Current manifect is: {0}", currentManifest);

			using (var manifestFile = storageContext.FileSystem.OpenForReading(currentManifest))
			{
				var logReader = new LogReader(manifestFile, true, 0, storageContext.Options.BufferPool);
				var builder = new Builder(storageContext, this, current);

				ulong? nextFileFromManifest = null;
				ulong? lastSequenceFromManifest = null;
				ulong? logNumberFromManifest = null;
				ulong? prevLogNumberFromManifest = null;

				Stream recordStream;
				while (logReader.TryReadRecord(out recordStream))
				{
					VersionEdit edit;
					using (recordStream)
					{
						edit = VersionEdit.DecodeFrom(recordStream);
					}

                    if (log.IsDebugEnabled)
                    {
                        log.Debug("Read version edit with the following information:\r\n{0}", edit.DebugInfo);
                    }

					if (edit.Comparator != storageContext.Options.Comparator.Name)
					{
						throw new InvalidOperationException(
							string.Format("Decoded version edit comparator '{0}' does not match '{1}' that is currently in use.",
										  edit.Comparator, storageContext.Options.Comparator.Name));
					}

					builder.Apply(edit);

					if (edit.NextFileNumber.HasValue)
					{
						nextFileFromManifest = edit.NextFileNumber;
					}
					if (edit.PrevLogNumber.HasValue)
					{
						prevLogNumberFromManifest = edit.PrevLogNumber;
					}
					if (edit.LogNumber.HasValue)
					{
						logNumberFromManifest = edit.LogNumber;
					}
					if (edit.LastSequence.HasValue)
					{
						lastSequenceFromManifest = edit.LastSequence;
					}
				}

				if (nextFileFromManifest == null)
				{
					throw new ManifestFileException("No NextFileNumber entry");
				}
				if (logNumberFromManifest == null)
				{
					throw new ManifestFileException("No LogNumber entry");
				}
				if (lastSequenceFromManifest == null)
				{
					throw new ManifestFileException("No LastSequenceNumber entry");
				}
				if (prevLogNumberFromManifest == null)
				{
					prevLogNumberFromManifest = 0;
				}

				MarkFileNumberUsed(prevLogNumberFromManifest.Value);
				MarkFileNumberUsed(logNumberFromManifest.Value);

				var version = new Version(storageContext, this);
				builder.SaveTo(version);
				Version.Finalize(version);
				AppendVersion(version);

				ManifestFileNumber = nextFileFromManifest.Value;
				NextFileNumber = nextFileFromManifest.Value + 1;
				LastSequence = lastSequenceFromManifest.Value;
				LogNumber = logNumberFromManifest.Value;
				PrevLogNumber = prevLogNumberFromManifest.Value;
			}
		}

		public void MarkFileNumberUsed(ulong number)
		{
			if (NextFileNumber <= number)
			{
				NextFileNumber = number + 1;
			}
		}
	}
}