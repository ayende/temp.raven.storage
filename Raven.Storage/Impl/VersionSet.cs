namespace Raven.Storage.Impl
{
	using System;
	using System.Collections.Generic;
	using System.Diagnostics;
	using System.Linq;

	using Raven.Storage.Comparing;
	using Raven.Storage.Data;

	public class VersionSet
	{
		private readonly StorageOptions options;

		private ulong _lastSequence;

		private Version _current;

		public Slice[] CompactionPointers { get; private set; }

		private readonly InternalKeyComparator internalKeyComparator;

		public VersionSet(StorageOptions options)
		{
			this.options = options;
			this.internalKeyComparator = new InternalKeyComparator(options.Comparator);

			NextFileNumber = 2;
			LogNumber = 0;
			ManifestFileNumber = 0;

			CompactionPointers = new Slice[Config.NumberOfLevels];

			this.AppendVersion(new Version(options, this));
		}

		/// <summary>
		/// Return the last sequence number.
		/// </summary>
		public ulong LastSequence
		{
			get { return _lastSequence; }
			set
			{
				Debug.Assert(value >= _lastSequence);
				_lastSequence = value;
			}
		}

		public Version Current
		{
			get
			{
				return _current;
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
				var v = _current;
				return v.CompactionScore >= 1 || v.FileToCompact != null;
			}
		}

		public ulong ManifestFileNumber { get; private set; }

		public ulong NextFileNumber { get; private set; }

		public int GetNumberOfFilesAtLevel(int level)
		{
			return _current.Files[level].Count;
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

		public void AddLiveFiles(IList<ulong> live)
		{
			throw new NotImplementedException();
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
			_current = version;
		}

		public Compaction PickCompaction()
		{
			throw new NotImplementedException();
		}

		public Compaction CompactRange(int level, Slice begin, Slice end)
		{
			var inputs = this.Current.GetOverlappingInputs(level, begin, end);
			if (inputs.Count == 0) 
				return null;

			var compaction = new Compaction(options, level, this.Current);
			compaction.Inputs[0] = inputs;

			SetupOtherInputs(compaction);

			return compaction;
		}

		private void SetupOtherInputs(Compaction compaction)
		{
			var level = compaction.Level;
			Slice smallestKey, largestKey;

			GetRange(compaction.Inputs[0], out smallestKey, out largestKey);

			compaction.Inputs[1] = Current.GetOverlappingInputs(level + 1, smallestKey, largestKey);

			Slice allStart, allLimit;
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
					Slice newStart, newLimit;
					GetRange(expanded0, out newStart, out newLimit);
					var expanded1 = Current.GetOverlappingInputs(level + 1, newStart, newLimit);

					if (expanded1.Count == compaction.Inputs[1].Count)
					{
						//Log(options_->info_log,
						//	"Expanding@%d %d+%d (%ld+%ld bytes) to %d+%d (%ld+%ld bytes)\n",
						//	level,
						//	int(c->inputs_[0].size()),
						//	int(c->inputs_[1].size()),
						//	long(inputs0_size), long(inputs1_size),
						//	int(expanded0.size()),
						//	int(expanded1.size()),
						//	long(expanded0_size), long(inputs1_size));

						smallestKey = newStart;
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
			CompactionPointers[level] = largestKey;
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
		private void GetRange(IReadOnlyList<FileMetadata> inputs, out Slice smallestKey, out Slice largestKey)
		{
			Debug.Assert(inputs.Count > 0);

			smallestKey = new Slice();
			largestKey = new Slice();

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
					if (internalKeyComparator.Compare(file.SmallestKey, smallestKey) < 0)
					{
						smallestKey = file.SmallestKey;
					}

					if (internalKeyComparator.Compare(file.LargestKey, largestKey) < 0)
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
		private void GetRange2(IEnumerable<FileMetadata> inputs1, IEnumerable<FileMetadata> inputs2, out Slice smallestKey, out Slice largestKey)
		{
			var all = new List<FileMetadata>(inputs1);
			all.AddRange(inputs2);

			GetRange(all, out smallestKey, out largestKey);
		}
	}
}