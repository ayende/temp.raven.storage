namespace Raven.Storage.Data
{
	using Raven.Storage.Impl;

	public class GetStats
	{
		public FileMetadata SeekFile { get; set; }

		public int SeekFileLevel { get; set; }
	}
}