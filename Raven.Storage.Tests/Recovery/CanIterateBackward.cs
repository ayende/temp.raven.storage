using System.IO;
using System.Text;
using System.Threading.Tasks;
using Raven.Storage.Data;
using Raven.Storage.Impl;
using Xunit;

namespace Raven.Storage.Tests.Recovery
{
	public class IterationAfterRecovery : StorageTestBase
	{
		[Fact]
		public async Task ShouldRecoverDataFromLogFile()
		{
			var storage = await NewStorageAsync();

			var name = storage.Name;

			var writeBatch = new WriteBatch();
			writeBatch.Put("A", new MemoryStream(Encoding.UTF8.GetBytes("123")));
			writeBatch.Put("B", new MemoryStream(Encoding.UTF8.GetBytes("123")));
			writeBatch.Put("D", new MemoryStream(Encoding.UTF8.GetBytes("123")));
			storage.Writer.WriteAsync(writeBatch).Wait();

			var fileSystem = storage.StorageState.FileSystem;

			storage.Dispose();

			using (var newStorage = new Storage(new StorageState(name, new StorageOptions())
			{
				FileSystem = fileSystem
			}))
			{
				await newStorage.InitAsync();
				
				using(var it = await  newStorage.Reader.NewIteratorAsync(new ReadOptions()))
				{
					it.Seek("C");
					Assert.True(it.IsValid);
					it.Prev();
					Assert.True(it.IsValid);
					Assert.Equal("B", it.Key);
				}
			}
		} 
	}
}