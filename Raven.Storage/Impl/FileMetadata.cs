namespace Raven.Storage.Impl
{
	using Raven.Storage.Data;

	public class FileMetadata
	{
		public long FileSize { get; set; }

		public ulong FileNumber { get; set; }

		public Slice SmallestKey { get; set; }

		public Slice LargestKey { get; set; }
	}
}