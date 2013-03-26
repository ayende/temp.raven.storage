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

		public ReadOptions()
		{
			FillCache = true;
		}
	}
}