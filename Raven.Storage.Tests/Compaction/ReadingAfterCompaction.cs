// -----------------------------------------------------------------------
//  <copyright file="ReadingAfterCompaction.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Threading.Tasks;
using Raven.Storage.Tests.Utils;
using Xunit;

namespace Raven.Storage.Tests.Compaction
{
	public class ReadingAfterCompaction : StorageTestBase
	{
		[Fact]
		public async Task GetLevel0Ordering()
		{
			// Check that we process level-0 files in correct order.  The code
			// below generates two level-0 files where the earlier one comes
			// before the later one in the level-0 file list since the earlier
			// one has a smaller "smallest" key.
			using (var storage = await NewStorageAsync())
			{
				storage.Put("bar", "b");
				storage.Put("foo", "v1");

				await storage.Commands.CompactMemTableAsync();

				storage.Put("foo", "v2");

				await storage.Commands.CompactMemTableAsync();

				Assert.Equal("v2", storage.Reader.Read("foo").AsString());
			}
		}

		[Fact]
		public async Task GetOrderedByLevels()
		{
			using (var storage = await NewStorageAsync())
			{
				storage.Put("foo", "v1");

				await storage.Commands.CompactMemTableAsync();
				await storage.Commands.CompactRangeAsync("a", "z");

				storage.Put("foo", "v2");

				await storage.Commands.CompactMemTableAsync();

				Assert.Equal("v2", storage.Reader.Read("foo").AsString());
			}
		}

		[Fact]
		public async Task GetPicksCorrectFile()
		{
			using (var storage = await NewStorageAsync())
			{
				// Arrange to have multiple files in a non-level-0 level.
				storage.Put("a", "va");
				await storage.Commands.CompactMemTableAsync();
				await storage.Commands.CompactRangeAsync("a", "b");

				storage.Put("x", "vx");
				await storage.Commands.CompactMemTableAsync();
				await storage.Commands.CompactRangeAsync("x", "y");

				storage.Put("f", "vf");
				await storage.Commands.CompactMemTableAsync();
				await storage.Commands.CompactRangeAsync("f", "g");

				Assert.Equal("va", storage.Reader.Read("a").AsString());
				Assert.Equal("vf", storage.Reader.Read("f").AsString());
				Assert.Equal("vx", storage.Reader.Read("x").AsString());
			}
		}
	}
}