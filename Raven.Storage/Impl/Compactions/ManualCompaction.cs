namespace Raven.Storage.Impl.Compactions
{
	using Raven.Storage.Data;

	/// <summary>
	/// Information for a manual compaction
	/// </summary>
	internal class ManualCompaction
	{
		public ManualCompaction(int level, InternalKey begin, InternalKey end)
		{
			this.Level = level;
			this.Begin = begin;
			this.End = end;
		}

		public int Level { get; private set; }

		public bool Done { get; set; }

		/// <summary>
		/// NULL means beginning of key range
		/// </summary>
		public InternalKey Begin { get; internal set; }

		/// <summary>
		/// NULL means end of key range
		/// </summary>
		public InternalKey End { get; private set; }
	}
}