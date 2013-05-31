namespace Raven.Storage.Impl
{
	using System;
	using System.Collections.Generic;
	using System.IO;
	using System.Linq;

	using Raven.Storage.Building;

	public class CompactionState : IDisposable
	{
		private readonly List<FileMetadata> outputs;

		public TableBuilder Builder { get; set; }

		public Compaction Compaction { get; private set; }

		public IReadOnlyList<FileMetadata> Outputs
		{
			get
			{
				return this.outputs.AsReadOnly();
			}
		}

		public FileMetadata CurrentOutput
		{
			get
			{
				return this.outputs.Last();
			}
		}

		public decimal SmallestSnapshot { get; set; }

		public Stream OutFile { get; set; }

		public long TotalBytes { get; set; }

		public CompactionState(Compaction compaction)
		{
			this.Compaction = compaction;
			this.SmallestSnapshot = -1;

			this.outputs = new List<FileMetadata>();
		}

		public void AddOutput(ulong fileNumber)
		{
			this.outputs.Add(new FileMetadata
			{
				FileNumber = fileNumber,
				SmallestKey = null,
				LargestKey = null,
				FileSize = 0
			});
		}

		public void Dispose()
		{
			if (Builder != null)
			{
				Builder.Dispose();
			}
			//else
			//{
			//	Debug.Assert(outFile == null);
			//}

			//if (outFile != null)
			//{
			//	outFile.Dispose();
			//}
		}
	}
}