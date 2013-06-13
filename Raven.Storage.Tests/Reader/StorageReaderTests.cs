namespace Raven.Storage.Tests.Reader
{
	using System.IO;
	using System.Text;

	using Xunit;

	public class StorageReaderTests : StorageTestBase
	{
		[Fact]
		public void ReadFromMemTable()
		{
			using (var storage = NewStorage())
			{
				var batch = new WriteBatch();
				batch.Put("test1", new MemoryStream(Encoding.UTF8.GetBytes("test")));
				storage.Writer.WriteAsync(batch).Wait();

				Assert.NotNull(storage.Reader.Read("test1"));
			}
		}

		[Fact]
		public void ReadFromImmutableMemTable()
		{
			using (var storage = this.NewStorage(new StorageOptions
				                                     {
					                                     WriteBatchSize = 1
				                                     }))
			{
				var batch1 = new WriteBatch();
				batch1.Put("test1", new MemoryStream(Encoding.UTF8.GetBytes("test")));
				storage.Writer.WriteAsync(batch1).Wait();

				var batch2 = new WriteBatch();
				batch2.Put("test2", new MemoryStream(Encoding.UTF8.GetBytes("test")));
				storage.Writer.WriteAsync(batch2).Wait();

				Assert.NotNull(storage.Reader.Read("test1"));
			}
		}
	}
}