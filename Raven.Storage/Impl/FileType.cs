namespace Raven.Storage.Impl
{
	public enum FileType
	{
		Unknown = 0,
		LogFile = 1,
		DBLockFile = 2,
		TableFile = 3,
		DescriptorFile = 4,
		CurrentFile = 5,
		TempFile = 6,
		InfoLogFile = 7	// Either the current one, or an old one
	}
}