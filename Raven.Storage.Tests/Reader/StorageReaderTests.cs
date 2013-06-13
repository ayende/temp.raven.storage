namespace Raven.Storage.Tests.Reader
{
	using System.IO;
	using System.Text;

	using Xunit;

	public class StorageReaderTests : StorageTestBase
	{
		[Fact]
		public async void ReadFromMemTable()
		{
			using (var storage = await NewStorageAsync())
			{
				var batch = new WriteBatch();
				batch.Put("test1", new MemoryStream(Encoding.UTF8.GetBytes("test")));
				await storage.Writer.WriteAsync(batch);

				Assert.NotNull(await storage.Reader.ReadAsync("test1"));
			}
		}

		[Fact]
		public async void ReadFromImmutableMemTable()
		{
			using (var storage = await NewStorageAsync(new StorageOptions
				                                     {
					                                     WriteBatchSize = 1
				                                     }))
			{
				var batch1 = new WriteBatch();
				batch1.Put("test1", new MemoryStream(Encoding.UTF8.GetBytes("test")));
				await storage.Writer.WriteAsync(batch1);

				var batch2 = new WriteBatch();
				batch2.Put("test2", new MemoryStream(Encoding.UTF8.GetBytes("test")));
				await storage.Writer.WriteAsync(batch2);

				Assert.NotNull(await storage.Reader.ReadAsync("test1"));
			}
		}
	}
}