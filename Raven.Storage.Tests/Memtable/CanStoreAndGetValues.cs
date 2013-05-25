using System.IO;
using Raven.Storage.Data;
using Raven.Storage.Memtable;
using Xunit;

namespace Raven.Storage.Tests.Memtable
{
	public class MemtableTests
	{
		[Fact]
		public void Empty()
		{
			StorageOptions storageOptions = new StorageOptions();
			using (var memtable = new MemTable(storageOptions))
			{
				Stream stream;
				Assert.False(memtable.TryGet("test", 1, out stream));
				Assert.Null(stream);
			}
		}

		[Fact]
		 public void CanAddAndGet()
		{
			StorageOptions storageOptions = new StorageOptions();
			using (var memtable = new MemTable(storageOptions))
			{
				memtable.Add(1, ItemType.Value, "test", memtable.Write(new MemoryStream(new byte[] { 1, 2, 3 })));
				Stream stream;
				Assert.True(memtable.TryGet("test", 1, out stream));
				using (stream)
				{
					Assert.Equal(1, stream.ReadByte());
					Assert.Equal(2, stream.ReadByte());
					Assert.Equal(3, stream.ReadByte());
				}
				
			}
		}

		[Fact]
		public void CanAddAndGetUsingLaterSnapshot()
		{
			StorageOptions storageOptions = new StorageOptions();
			using (var memtable = new MemTable(storageOptions))
			{
				memtable.Add(1, ItemType.Value, "test", memtable.Write(new MemoryStream(new byte[] { 1, 2, 3 })));
				Stream stream;
				Assert.True(memtable.TryGet("test", 2, out stream));
				using (stream)
				{
					Assert.Equal(1, stream.ReadByte());
					Assert.Equal(2, stream.ReadByte());
					Assert.Equal(3, stream.ReadByte());
				}

			}
		}

		[Fact]
		public void WillNotShowValueFromLaterSnapshot()
		{
			StorageOptions storageOptions = new StorageOptions();
			using (var memtable = new MemTable(storageOptions))
			{
				memtable.Add(2, ItemType.Value, "test", memtable.Write(new MemoryStream(new byte[] { 1, 2, 3 })));
				Stream stream;
				Assert.False(memtable.TryGet("test", 1, out stream));
			}
		}


		[Fact]
		public void DeletesWillHideValues()
		{
			StorageOptions storageOptions = new StorageOptions();
			using (var memtable = new MemTable(storageOptions))
			{
				memtable.Add(2, ItemType.Value, "test", memtable.Write(new MemoryStream(new byte[] { 1, 2, 3 })));
				memtable.Add(3, ItemType.Deletion, "test", null);

				Stream stream;
				Assert.True(memtable.TryGet("test", 5, out stream));
				Assert.Null(stream);
			}
		}
	}
}