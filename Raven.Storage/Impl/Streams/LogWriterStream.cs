namespace Raven.Storage.Impl.Streams
{
	public enum LogRecordType : byte
	{
		ZeroType = 0, // reserved for zero allocated file
		FullType = 1,

		// fragments
		StartType = 2,
		MiddleType = 3,
		EndType = 4
	}
}