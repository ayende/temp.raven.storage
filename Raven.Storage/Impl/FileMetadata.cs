namespace Raven.Storage.Impl
{
	using Raven.Storage.Data;

	public class FileMetadata
	{
		public FileMetadata()
		{
			this.AllowedSeeks = 1 << 30; // 1073741824
		}

		public FileMetadata(FileMetadata file)
		{
			this.FileSize = file.FileSize;
			this.FileNumber = file.FileNumber;
			this.LargestKey = file.LargestKey;
			this.SmallestKey = file.SmallestKey;
		}

		/// <summary>
		/// Seeks allowed until compaction
		/// </summary>
		public long AllowedSeeks { get; set; }

		public long FileSize { get; set; }

		public ulong FileNumber { get; set; }

		public Slice SmallestKey { get; set; }

		public Slice LargestKey { get; set; }
	}
}