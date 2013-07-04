namespace Raven.Storage.Tests
{
	using System;
	using System.Threading.Tasks;

	using Raven.Storage.Benchmark.Generators;
	using Raven.Storage.Data;

	using Xunit;

	public class IteratorTests
	{
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