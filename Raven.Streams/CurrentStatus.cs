using System.Collections.Generic;
using System.IO;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Storage.Data;
using Raven.Storage.Memtable;

namespace Raven.Streams
{
	public class CurrentStatus
	{
		private long currentEtag;
		private long fileBase;
		private long logBase;
		private long nextSeq;
		private long etagBase;

		public long Version { get; set; }

		public long LogBase
		{
			get { return logBase; }
			set { logBase = value; }
		}

		public long FileBase
		{
			get { return fileBase; }
			set { fileBase = value; }
		}

		public long NextSeq
		{
			get { return nextSeq; }
			set { nextSeq = value; }
		}

		public long EtagBase
		{
			get { return etagBase; }
			set { etagBase = value; }
		}

		public Etag NextEtag()
		{
			return new Etag(UuidType.Documents, etagBase, Interlocked.Increment(ref currentEtag));
		}

		public ulong NextSeqeunce()
		{
			return (ulong)Interlocked.Increment(ref nextSeq);
		}

		public string CreateNewTableFileName()
		{
			var counter = Interlocked.Increment(ref fileBase);
			return string.Format("0-{0:00000000}.sst", counter);
		}

		public string CreateNewLogFileName(out long log)
		{
			log = Interlocked.Increment(ref logBase);
			return string.Format("{0:00000000}.log", log);
		}

		public List<SstRange> Ranges { get; set; }

		public long LastCompletedLog { get; set; }

		public CurrentStatus()
		{
			Ranges = new List<SstRange>();
		}


		public class SstRange
		{
			public Slice Start { get; set; }
			public Slice End { get; set; }
			public string Name { get; set; }
			public int Count { get; set; }
		}
	}

}