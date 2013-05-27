namespace Raven.Storage.Impl
{
	using System;
	using System.Collections.Generic;
	using System.Diagnostics;

	using Raven.Storage.Data;

	public class VersionSet
	{
		private ulong _lastSequence;
		private Version _current = new Version();

		public Slice[] CompactionPointers { get; private set; }

		public VersionSet()
		{
			NextFileNumber = 2;
			LogNumber = 0;
			ManifestFileNumber = 0;

			CompactionPointers = new Slice[Config.NumberOfLevels];
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
			throw new NotImplementedException();
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
	}
}