namespace Raven.Storage.Impl.Compactions
{
	using Raven.Storage.Data;

	/// <summary>
	/// Information for a manual compaction
	/// </summary>
	public class ManualCompaction
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
	}
}