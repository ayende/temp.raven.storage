namespace Raven.Storage.Tests.Compactions
{
	using System.IO;
	using System.Text;

	using Xunit;

	public class CompactionTests : StorageTestBase
	{
		[Fact]
		public void T1()
		{
			using (var storage = NewStorage())
			{
				for (int i = 0; i < 3; i++)
				{
					var writeBatch = new WriteBatch();
					writeBatch.Put("p", new MemoryStream(Encoding.UTF8.GetBytes("begin")));
					storage.Writer.WriteAsync(writeBatch).Wait();

					writeBatch = new WriteBatch();
					writeBatch.Put("q", new MemoryStream(Encoding.UTF8.GetBytes("end")));
					storage.Writer.WriteAsync(writeBatch).Wait();

					storage.Commands.Compact(0, "p", "q");
				}
			}
		}
	}
}
