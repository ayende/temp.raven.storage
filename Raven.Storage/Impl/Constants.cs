namespace Raven.Storage.Impl
{
	public class Constants
	{
		public class Files
		{
			public const string CurrentFile = "CURRENT";

			public const string DBLockFile = "LOCK";

			public const string LogFile = "LOG";

			public const string ManifestPrefix = "MANIFEST-";

			public class Extensions
			{
				public const string LogFile = ".log";

				public const string TableFile = ".sst";

				public const string TempFile = ".dbtmp";
			}
		}
	}
}