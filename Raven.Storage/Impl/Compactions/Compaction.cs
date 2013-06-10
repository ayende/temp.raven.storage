namespace Raven.Storage.Impl.Compactions
{
	using System.Collections.Generic;
	using System.Linq;

	using Raven.Storage.Data;

	public class Compaction
	{
		/// <summary>
		/// Return the level that is being compacted.  Inputs from "level"
		/// and "level+1" will be merged to produce a set of "level+1" files.
		/// </summary>
		public int Level { get; private set; }

		/// <summary>
		/// Maximum size of files to build during this compaction.
		/// </summary>
		public int MaxOutputFileSize { get; private set; }

		private Version inputVersion;

		/// <summary>
		/// Return the object that holds the edits to the descriptor done
		/// by this compaction.
		/// </summary>
		public VersionEdit Edit { get; private set; }

		/// <summary>
		/// Each compaction reads inputs from "level_" and "level_+1"
		/// </summary>
		public List<FileMetadata>[] Inputs { get; set; }

		/// <summary>
		/// State used to check for number of of overlapping grandparent files
		/// (parent == level_ + 1, grandparent == level_ + 2)
		/// </summary>
		public List<FileMetadata> Grandparents { get; set; }

		/// <summary>
		/// Index in grandparent_starts_
		/// </summary>
		private int grandparentIndex;

		/// <summary>
		/// Some output key has been seen
		/// </summary>
		private bool seenKey;

		/// <summary>
		/// Bytes of overlap between current output
		/// and grandparent files
		/// </summary>
		private long overlappedBytes;

		/// <summary>
		/// level_ptrs_ holds indices into input_version_->levels_: our state
		/// is that we are positioned at one of the file ranges for each
		/// higher level than the ones involved in this compaction (i.e. for
		/// all L >= level_ + 2).
		/// </summary>
		private readonly int[] levelPointers;

		private readonly IStorageContext storageContext;

		public Compaction(IStorageContext storageContext, int level, Version inputVersion = null)
		{
			this.storageContext = storageContext;
			this.Level = level;
			this.MaxOutputFileSize = Config.TargetFileSize;
			this.inputVersion = inputVersion;
			this.Edit = new VersionEdit();
			this.Inputs = new List<FileMetadata>[2];
			this.Grandparents = new List<FileMetadata>();
			this.grandparentIndex = 0;
			this.seenKey = false;
			this.overlappedBytes = 0;

			this.levelPointers = new int[Config.NumberOfLevels];

			for (int lvl = 0; lvl < Config.NumberOfLevels; lvl++)
			{
				this.levelPointers[lvl] = 0;
			}
		}

		/// <summary>
		/// "which" must be either 0 or 1
		/// </summary>
		/// <param name="which"></param>
		/// <returns></returns>
		public int GetNumberOfInputFiles(int which)
		{
			return this.Inputs[which].Count;
		}

		/// <summary>
		/// Return the ith input file at "level()+which" ("which" must be 0 or 1)
		/// </summary>
		/// <param name="which"></param>
		/// <param name="i"></param>
		/// <returns></returns>
		public FileMetadata GetInput(int which, int i)
		{
			return this.Inputs[which][i];
		}

		/// <summary>
		/// Is this a trivial compaction that can be implemented by just
		/// moving a single input file to the next level (no merging or splitting)
		/// </summary>
		/// <returns></returns>
		public bool IsTrivialMove()
		{
			return (this.GetNumberOfInputFiles(0) == 1 && this.GetNumberOfInputFiles(1) == 0
					&& this.Grandparents.Sum(x => x.FileSize) <= Config.MaxGrandParentOverlapBytes);
		}

		/// <summary>
		/// Add all inputs to this compaction as delete operations to *edit.
		/// </summary>
		/// <param name="edit"></param>
		public void AddInputDeletions(VersionEdit edit)
		{
			for (var which = 0; which < 2; which++)
			{
				for (var i = 0; i < this.Inputs[which].Count; i++)
				{
					edit.DeleteFile(this.Level + which, this.Inputs[which][i].FileNumber);
				}
			}
		}

		/// <summary>
		/// Returns true if the information we have available guarantees that
		/// the compaction is producing data in "level+1" for which no data exists
		/// in levels greater than "level+1".
		/// </summary>
		/// <param name="userKey"></param>
		/// <returns></returns>
		public bool IsBaseLevelForKey(Slice userKey)
		{
			// Maybe use binary search to find right entry instead of linear search?
			var userComparator = storageContext.InternalKeyComparator.UserComparator;
			for (int lvl = this.Level + 2; lvl < Config.NumberOfLevels; lvl++)
			{
				var files = this.inputVersion.Files[lvl];
				for (; this.levelPointers[lvl] < files.Count; )
				{
					var file = files[this.levelPointers[lvl]];
					if (userComparator.Compare(userKey, file.LargestKey.UserKey) <= 0)
					{
						// We've advanced far enough
						if (userComparator.Compare(userKey, file.SmallestKey.UserKey) >= 0)
						{
							return false;
						}

						break;
					}

					this.levelPointers[lvl]++;
				}
			}

			return true;
		}

		/// <summary>
		/// Returns true iff we should stop building the current output
		/// before processing "internal_key".
		/// </summary>
		/// <param name="internalKey"></param>
		/// <returns></returns>
		public bool ShouldStopBefore(Slice internalKey)
		{
			while (this.grandparentIndex < this.Grandparents.Count
				   && storageContext.InternalKeyComparator.Compare(internalKey, this.Grandparents[this.grandparentIndex].LargestKey.Encode()) > 0)
			{
				if (this.seenKey)
				{
					this.overlappedBytes += this.Grandparents[this.grandparentIndex].FileSize;
				}

				this.grandparentIndex++;
			}

			this.seenKey = true;

			if (this.overlappedBytes > Config.MaxGrandParentOverlapBytes)
			{
				// Too much overlap for current output; start new output
				this.overlappedBytes = 0;
				return true;
			}

			return false;
		}

		/// <summary>
		/// Release the input version for the compaction, once the compaction
		/// is successful.
		/// </summary>
		/// <returns></returns>
		public void ReleaseInputs()
		{
			this.inputVersion = null;
		}
	}
}