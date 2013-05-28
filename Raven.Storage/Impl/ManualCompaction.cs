namespace Raven.Storage.Impl
{
	using Raven.Storage.Data;

	/// <summary>
	/// Information for a manual compaction
	/// </summary>
	public struct ManualCompaction
	{
		public int Level { get; set; }

		public bool Done { get; set; }

		/// <summary>
		/// NULL means beginning of key range
		/// </summary>
		public Slice Begin { get; set; }

		/// <summary>
		/// NULL means end of key range
		/// </summary>
		public Slice End { get; set; }

		/// <summary>
		/// Used to keep track of compaction progress
		/// </summary>
		public Slice TmpStorage { get; set; }
	}
}