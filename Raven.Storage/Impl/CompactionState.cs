namespace Raven.Storage.Impl
{
	using System;
	using System.Collections.Generic;
	using System.Diagnostics;

	using Raven.Storage.Building;
	using Raven.Storage.Data;

	public class CompactionState : IDisposable
	{
		private readonly List<Output> outputs;

		public TableBuilder Builder { get; set; }

		public Compaction Compaction { get; private set; }

		public IReadOnlyList<Output> Outputs
		{
			get
			{
				return this.outputs.AsReadOnly();
			}
		}

		public decimal SmallestSnapshot { get; private set; }

		public CompactionState(Compaction compaction)
		{
			this.Compaction = compaction;
			this.SmallestSnapshot = -1;

			this.outputs = new List<Output>();
		}

		public struct Output
		{
			public ulong FileNumber { get; set; }

			public long FileSize { get; set; }

			public Slice SmallestKey { get; set; }

			public Slice LargestKey { get; set; }
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