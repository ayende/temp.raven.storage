namespace Raven.Storage.Tests.Reader
{
	using System.IO;
	using System.Text;

	using Xunit;

	public class StorageReaderTests
	{
		[Fact]
		public void ReadFromMemTable()
		{
			using (var storage = new Storage("test", new StorageOptions()))
			{
				var batch = new WriteBatch();
				batch.Put("test1", new MemoryStream(Encoding.UTF8.GetBytes("test")));
				storage.Writer.Write(batch);

				Assert.NotNull(storage.Reader.Read("test1"));
			}
		}

		[Fact]
		public void ReadFromImmutableMemTable()
		{
			using (var storage = new Storage("test", new StorageOptions
														 {
															 WriteBatchSize = 1
														 }))
			{
				var batch1 = new WriteBatch();
				batch1.Put("test1", new MemoryStream(Encoding.UTF8.GetBytes("test")));
				storage.Writer.Write(batch1);

				var batch2 = new WriteBatch();
				batch2.Put("test2", new MemoryStream(Encoding.UTF8.GetBytes("test")));
				storage.Writer.Write(batch2);

				Assert.NotNull(storage.Reader.Read("test1"));
			}
		}
	}
}