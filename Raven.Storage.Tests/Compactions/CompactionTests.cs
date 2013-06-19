using System.Threading.Tasks;

namespace Raven.Storage.Tests.Compactions
{
	using System;
	using System.IO;
	using System.Text;

	using Xunit;

	public class CompactionTests : StorageTestBase
	{
		[Fact]
		public async Task CompactMemTableShouldCreateFilesAtDifferentLevels()
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

					await storage.Commands.CompactMemTableAsync();
				}

				var statistics = await storage.Commands.GetStatisticsAsync();

				AssertNumberOfFilesPerLevel(statistics, 1, 1, 1, 0, 0, 0, 0);
			}
		}

		private void AssertNumberOfFilesPerLevel(StorageStatistics statistics, params int[] numberOfFilesPerLevel)
		{
			for (var level = 0; level < numberOfFilesPerLevel.Length; level++)
			{
				Assert.Equal(numberOfFilesPerLevel[level], statistics.Files[level].Count);
			}
		}
	}
}
