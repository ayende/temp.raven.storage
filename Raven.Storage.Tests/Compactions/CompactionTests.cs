namespace Raven.Storage.Tests.Compactions
{
	using System.IO;
	using System.Text;

	using Xunit;

	public class CompactionTests : StorageTestBase
	{
		[Fact]
		public async void T1()
		{
			using (var storage = await NewStorageAsync())
			{
				for (int i = 0; i < 3; i++)
				{
					var writeBatch = new WriteBatch();
					writeBatch.Put("p", new MemoryStream(Encoding.UTF8.GetBytes("begin")));
					await storage.Writer.WriteAsync(writeBatch);

					writeBatch = new WriteBatch();
					writeBatch.Put("q", new MemoryStream(Encoding.UTF8.GetBytes("end")));
					await storage.Writer.WriteAsync(writeBatch);

					await storage.Commands.CompactAsync(0, "p", "q");
				}
			}
		}
	}
}
