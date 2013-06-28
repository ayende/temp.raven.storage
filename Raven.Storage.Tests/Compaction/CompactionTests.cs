using System.Threading.Tasks;
using Raven.Storage.Impl;
using Raven.Storage.Tests.Recovery;
using Raven.Storage.Tests.Utils;

namespace Raven.Storage.Tests.Compaction
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
				Assert.Equal(Config.MaxMemCompactLevel, 2); // Just to make sure - here we relay on that

				await MakeTables(storage, 3, "p", "q");

				var statistics = await storage.Commands.GetStatisticsAsync();
				AssertNumberOfFilesPerLevel(statistics, 1, 1, 1, 0, 0, 0, 0);

				// Compaction range falls before files
				await storage.Commands.CompactRangeAsync("", "c");
				statistics = await storage.Commands.GetStatisticsAsync();
				AssertNumberOfFilesPerLevel(statistics, 1, 1, 1, 0, 0, 0, 0);

				// Compaction range falls after files
				await storage.Commands.CompactRangeAsync("r", "z");
				statistics = await storage.Commands.GetStatisticsAsync();
				AssertNumberOfFilesPerLevel(statistics, 1, 1, 1, 0, 0, 0, 0);

				// Compaction range overlaps files
				await storage.Commands.CompactRangeAsync("p1", "p9");
				statistics = await storage.Commands.GetStatisticsAsync();
				AssertNumberOfFilesPerLevel(statistics, 0, 0, 1, 0, 0, 0, 0);

				// Populate a different range
				await MakeTables(storage, 3, "c", "e");

				statistics = await storage.Commands.GetStatisticsAsync();
				AssertNumberOfFilesPerLevel(statistics, 1, 1, 2, 0, 0, 0, 0);

				// Compact just the new range
				await storage.Commands.CompactRangeAsync("b", "f");
				statistics = await storage.Commands.GetStatisticsAsync();
				AssertNumberOfFilesPerLevel(statistics, 0, 0, 2, 0, 0, 0, 0);

				// Compact all
				await MakeTables(storage, 1, "a", "z");
				statistics = await storage.Commands.GetStatisticsAsync();
				AssertNumberOfFilesPerLevel(statistics, 0, 1, 2, 0, 0, 0, 0);
				await storage.Commands.CompactRangeAsync(null, null);
				statistics = await storage.Commands.GetStatisticsAsync();
				AssertNumberOfFilesPerLevel(statistics, 0, 0, 1, 0, 0, 0, 0);
			}
		}

		private async Task MakeTables(Storage storage, int n, string small, string large)
		{
			for (int i = 0; i < n; i++)
			{
				var writeBatch = new WriteBatch();
				writeBatch.Put(small, new MemoryStream(Encoding.UTF8.GetBytes("begin")));
				await storage.Writer.WriteAsync(writeBatch);

				writeBatch = new WriteBatch();
				writeBatch.Put(large, new MemoryStream(Encoding.UTF8.GetBytes("end")));
				await storage.Writer.WriteAsync(writeBatch);

				await storage.Commands.CompactMemTableAsync();
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
