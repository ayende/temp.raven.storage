namespace Raven.Storage.Data
{
	public class WriteOptions
	{
		public WriteOptions()
		{
			this.FlushToDisk = true;
		}

		public bool FlushToDisk { get; set; }
	}
}