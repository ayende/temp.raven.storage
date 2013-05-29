namespace Raven.Storage.Impl
{
	/// <summary>
	/// Tag numbers for serialized VersionEdit.  These numbers are written to
	/// disk and should not be changed.
	/// </summary>
	public enum Tag
	{
		Comparator = 1,
		LogNumber = 2,
		NextFileNumber = 3,
		LastSequence = 4,
		CompactPointer = 5,
		DeletedFile = 6,
		NewFile = 7,
		// 8 was used for large value refs
		PrevLogNumber = 9
	}
}