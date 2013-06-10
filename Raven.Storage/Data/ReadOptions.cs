namespace Raven.Storage.Data
{
	public class ReadOptions
	{
		/// <summary>
		/// If true, all data read from underlying storage will be
		/// verified against corresponding checksums.
		/// Default: false
		/// </summary>
		public bool VerifyChecksums { get; set; }

		/// <summary>
		/// Should the data read for this iteration be cached in memory?
		/// Callers may wish to set this field to false for bulk scans.
		/// Default: true
		/// </summary>
		public bool FillCache { get; set; }

		/// <summary>
		/// If "snapshot" is non-NULL, read as of the supplied snapshot
		/// (which must belong to the DB that is being read and which must
		/// not have been released).  If "snapshot" is NULL, use an impliicit
		/// snapshot of the state at the beginning of this read operation.
		/// Default: NULL
		/// </summary>
		public Snapshot Snapshot { get; set; }

		public ReadOptions()
		{
			FillCache = true;
			Snapshot = null;
		}
	}
}