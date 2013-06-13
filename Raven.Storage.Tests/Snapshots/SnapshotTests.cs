namespace Raven.Storage.Tests.Snapshots
{
	using System.IO;
	using System.Text;

	using Raven.Storage.Data;

	using Xunit;

	public class SnapshotTests : StorageTestBase
	{
		[Fact]
		public async void SnapshotTest()
		{
			using (var storage = await NewStorageAsync())
			{
				var str1 = "test1";
				var str2 = "test2";

				var s1 = new MemoryStream(Encoding.UTF8.GetBytes(str1));
				var s2 = new MemoryStream(Encoding.UTF8.GetBytes(str2));

				var writeBatch = new WriteBatch();
				writeBatch.Put("key1", s1);

				await storage.Writer.WriteAsync(writeBatch);

				var snapshot = await storage.Commands.CreateSnapshotAsync();

				writeBatch = new WriteBatch();
				writeBatch.Put("key1", s2);

				await storage.Writer.WriteAsync(writeBatch);

				AssertEqual(str2, storage.Reader.Read("key1"));
				AssertEqual(str1, storage.Reader.Read("key1", new ReadOptions
					                                                  {
						                                                  Snapshot = snapshot
					                                                  }));

				await storage.Commands.ReleaseSnapshotAsync(snapshot);
			}
		}

		[Fact]
		public async void SnapshotWithCompactionTest()
		{
			using (var storage = await NewStorageAsync(new StorageOptions
				                                {
					                                WriteBatchSize = 1
				                                }))
			{
				var str1 = "test1";
				var str2 = "test2";

				var s1 = new MemoryStream(Encoding.UTF8.GetBytes(str1));
				var s2 = new MemoryStream(Encoding.UTF8.GetBytes(str2));

				var writeBatch = new WriteBatch();
				writeBatch.Put("key1", s1);

				await storage.Writer.WriteAsync(writeBatch);

				var snapshot = await storage.Commands.CreateSnapshotAsync();

				writeBatch = new WriteBatch();
				writeBatch.Put("key1", s2);

				await storage.Writer.WriteAsync(writeBatch);

				AssertEqual(str2, storage.Reader.Read("key1"));
				AssertEqual(str1, storage.Reader.Read("key1", new ReadOptions
				{
					Snapshot = snapshot
				}));

				await storage.Commands.CompactAsync(0, "key1", "key1");

				AssertEqual(str2, storage.Reader.Read("key1"));
				AssertEqual(str1, storage.Reader.Read("key1", new ReadOptions
				{
					Snapshot = snapshot
				}));

				await storage.Commands.ReleaseSnapshotAsync(snapshot);
			}
		}

		public void AssertEqual(string expected, Stream actual)
		{
			actual.Position = 0;

			using (var reader = new StreamReader(actual))
			{
				var str = reader.ReadToEnd();

				Assert.Equal(expected, str);
			}
		}
	}
}