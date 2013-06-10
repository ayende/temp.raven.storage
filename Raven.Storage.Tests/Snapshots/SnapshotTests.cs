namespace Raven.Storage.Tests.Snapshots
{
	using System.IO;
	using System.Text;

	using Raven.Storage.Data;

	using Xunit;

	public class SnapshotTests : StorageTestBase
	{
		[Fact]
		public void SnapshotTest()
		{
			using (var storage = NewStorage())
			{
				var str1 = "test1";
				var str2 = "test2";

				var s1 = new MemoryStream(Encoding.UTF8.GetBytes(str1));
				var s2 = new MemoryStream(Encoding.UTF8.GetBytes(str2));

				var writeBatch = new WriteBatch();
				writeBatch.Put("key1", s1);

				storage.Writer.Write(writeBatch);

				var snapshot = storage.Commands.CreateSnapshot();

				writeBatch = new WriteBatch();
				writeBatch.Put("key1", s2);

				storage.Writer.Write(writeBatch);

				AssertEqual(str2, storage.Reader.Read("key1"));
				AssertEqual(str1, storage.Reader.Read("key1", new ReadOptions
					                                                  {
						                                                  Snapshot = snapshot
					                                                  }));

				storage.Commands.ReleaseSnapshot(snapshot);
			}
		}

		[Fact]
		public void SnapshotWithCompactionTest()
		{
			using (var storage = NewStorage(new StorageOptions
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

				storage.Writer.Write(writeBatch);

				var snapshot = storage.Commands.CreateSnapshot();

				writeBatch = new WriteBatch();
				writeBatch.Put("key1", s2);

				storage.Writer.Write(writeBatch);

				AssertEqual(str2, storage.Reader.Read("key1"));
				AssertEqual(str1, storage.Reader.Read("key1", new ReadOptions
				{
					Snapshot = snapshot
				}));

				storage.Commands.Compact(0, "key1", "key1");

				AssertEqual(str2, storage.Reader.Read("key1"));
				AssertEqual(str1, storage.Reader.Read("key1", new ReadOptions
				{
					Snapshot = snapshot
				}));

				storage.Commands.ReleaseSnapshot(snapshot);
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