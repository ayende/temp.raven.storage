namespace Raven.Storage.Impl
{
	public static class Config
	{
		/// <summary>
		/// Soft limit on number of level-0 files.  We slow down writes at this point.
		/// </summary>
		public const int SlowdownWritesTrigger = 8;

		/// <summary>
		/// Maximum number of level-0 files.  We stop writes at this point.
		/// </summary>
		public const int StopWritesTrigger = 12;
	}
}