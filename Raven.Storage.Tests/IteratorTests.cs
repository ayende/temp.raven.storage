namespace Raven.Storage.Tests
{
	using System;
	using System.Collections.Generic;
	using System.IO;
	using System.Linq;
	using System.Threading.Tasks;

	using Raven.Storage.Benchmark.Generators;
	using Raven.Storage.Comparing;
	using Raven.Storage.Data;
	using Raven.Storage.Impl;
	using Raven.Storage.Memtable;
	using Raven.Storage.Reading;

	using Xunit;

	public class IteratorTests
	{
		[Fact]
		public void T1()
		{
			var storageState = new StorageState("test", new StorageOptions());
			var random = new Random();
			var tables = new List<MemTable>();
			var expectedCount = 0;

			try
			{
				ulong seq = 0;
				for (var i = 0; i < 100; i++)
				{
					var table = new MemTable(storageState);
					for (var j = 0; j < 1000; j++)
					{
						var k = random.Next();
						var key = string.Format("{0:0000000000000000}", k);
						table.Add(seq++, ItemType.Value, key, null);
						expectedCount++;
					}

					tables.Add(table);
				}

				var iterators = tables.Select(table => table.NewIterator()).ToList();
				var comparator = new InternalKeyComparator(new CaseInsensitiveComparator());

				using (var iterator = new MergingIterator(comparator, iterators))
				{
					var actualCount = 0;
					iterator.SeekToFirst();
					Assert.True(iterator.IsValid);

					Slice prev = string.Empty;

					while (iterator.IsValid)
					{
						if (!prev.IsEmpty())
						{
							Assert.True(comparator.Compare(iterator.Key, prev) > 0);
						}

						prev = iterator.Key.Clone();
						iterator.Next();
						actualCount++;
					}

					Assert.Equal(expectedCount, actualCount);
				}
			}
			finally
			{
				foreach (var table in tables)
					table.Dispose();
			}

			
		}

		[Fact]
		public async Task DbIteratorShouldWorkCorrectlyAfterCompaction()
		{
			using (var storage = new Storage(Guid.NewGuid().ToString(), new StorageOptions()))
			{
				await storage.InitAsync();
				await this.DoWrite(storage, 10000, true);
				await storage.Commands.CompactMemTableAsync();
				var iterator = storage.Reader.NewIterator(new ReadOptions());
				iterator.SeekToFirst();
				int i = 0;
				while (iterator.IsValid)
				{
					i++;

					iterator.Next();
				}

				Assert.Equal(10000, i);
			}
		}

		private async Task DoWrite(Storage storage, int num, bool seq)
		{
			var random = new Random();
			var generator = new RandomGenerator();

			for (var i = 0; i < num; i += 1)
			{
				var batch = new WriteBatch();
				for (var j = 0; j < 1; j++)
				{
					var k = seq ? i + j : random.Next() % num;
					var key = string.Format("{0:0000000000000000}", k);
					batch.Put(key, generator.Generate(100));
				}

				await storage.Writer.WriteAsync(batch);
			}
		}
	}
}