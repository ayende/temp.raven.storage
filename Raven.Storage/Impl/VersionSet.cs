using System;
using System.Diagnostics;

namespace Raven.Storage.Impl
{
	using System.Collections.Generic;

	public class VersionSet
	{
		private ulong _lastSequence;
		private Version _current = new Version();

		private ulong nextFileNumber;

		public VersionSet()
		{
			nextFileNumber = 2;
			LogNumber = 0;
			ManifestFileNumber = 0;
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
		public ulong PrevLogNumber { get; set; }

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

		public int GetNumberOfFilesAtLevel(int level)
		{
			throw new NotImplementedException();
		}

		public ulong NewFileNumber()
		{
			return nextFileNumber++;
		}

		public void ReuseFileNumber(ulong number)
		{
			if (nextFileNumber == number + 1)
			{
				nextFileNumber = number;
			}
		}

		public Status LogAndApply(VersionEdit edit)
		{
			throw new NotImplementedException();
		}

		public void AddLiveFiles(IList<ulong> live)
		{
			throw new NotImplementedException();
		}
	}
}