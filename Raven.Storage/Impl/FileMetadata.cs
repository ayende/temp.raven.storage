namespace Raven.Storage.Impl
{
	using Raven.Storage.Data;

	public class FileMetadata
	{
		public FileMetadata()
		{
			AllowedSeeks = 1 << 30; // 1073741824
		}

		public FileMetadata(FileMetadata file)
		{
			FileSize = file.FileSize;
			FileNumber = file.FileNumber;
			LargestKey = file.LargestKey;
			SmallestKey = file.SmallestKey;
		}

		/// <summary>
		/// Seeks allowed until compaction
		/// </summary>
		public long AllowedSeeks { get; set; }

		public long FileSize { get; set; }

		public ulong FileNumber { get; set; }

		public InternalKey SmallestKey { get; set; }

		public InternalKey LargestKey { get; set; }
	}
}