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
			using (var memtable = new MemTable(new StorageOptions()))
			{
				Stream stream;
				Assert.False(memtable.TryGet("test", 1, out stream));
				Assert.Null(stream);
			}
		}

		[Fact]
		 public void CanAddAndGet()
		 {
			using (var memtable = new MemTable(new StorageOptions()))
			{
				memtable.Add(1, ItemType.Value, "test", new MemoryStream(new byte[] {1, 2, 3}));
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
			using (var memtable = new MemTable(new StorageOptions()))
			{
				memtable.Add(1, ItemType.Value, "test", new MemoryStream(new byte[] { 1, 2, 3 }));
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
			using (var memtable = new MemTable(new StorageOptions()))
			{
				memtable.Add(2, ItemType.Value, "test", new MemoryStream(new byte[] { 1, 2, 3 }));
				Stream stream;
				Assert.False(memtable.TryGet("test", 1, out stream));
			}
		}


		[Fact]
		public void DeletesWillHideValues()
		{
			using (var memtable = new MemTable(new StorageOptions()))
			{
				memtable.Add(2, ItemType.Value, "test", new MemoryStream(new byte[] { 1, 2, 3 }));
				memtable.Add(3, ItemType.Deletion, "test", null);

				Stream stream;
				Assert.True(memtable.TryGet("test", 5, out stream));
				Assert.Null(stream);
			}
		}
	}
}