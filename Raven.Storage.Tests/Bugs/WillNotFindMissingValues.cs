using System.IO;
using System.Threading.Tasks;
using Raven.Storage.Comparing;
using Xunit;

namespace Raven.Storage.Tests.Bugs
{
	public class WillNotFindMissingValues : StorageTestBase
	{
		[Fact]
		public async Task WillNotFindMissingValue()
		{
			using (var storage = await NewStorageAsync(new StorageOptions()))
			{
				var writeBatch = new WriteBatch();
				writeBatch.Put("system/config", new MemoryStream());
				await storage.Writer.WriteAsync(writeBatch);

				Assert.Null(storage.Reader.Read("system/Test"));
			}
		}
	}
}