namespace Raven.Storage.Data
{
	public class WriteOptions
	{
		public WriteOptions()
		{
			FlushToDisk = true;
		}

		public bool FlushToDisk { get; set; } 
	}
}