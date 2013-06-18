using System.Threading.Tasks;
using Raven.Abstractions.Indexing;
using Raven.Storage.Impl;
using Xunit;

namespace Raven.Aggregation.Tests
{
    public class AggregationsCreation
    {
		[Fact]
		public async Task CanAdd()
		{
			using (var agg = new AggregationEngine())
			{
				await agg.InitAsync();
				await agg.CreateAggregationAsync(new IndexDefinition
					{
						Name = "Test",
						Map = "from doc in docs select new { Count = 1}",
						Reduce = "from result in results group result by 1 into g select new { Count = g.Sum(x=>x.Count) }"
					});
				Assert.NotNull(agg.GetAggregation("Test"));
			}
		}

		[Fact]
		public async Task CanAddAndRememberAfterRestart()
		{
			FileSystem fileSystem;
			using (var agg = new AggregationEngine())
			{
				await agg.InitAsync();
				await agg.CreateAggregationAsync(new IndexDefinition
				{
					Name = "Test",
					Map = "from doc in docs select new { Count = 1}",
					Reduce = "from result in results group result by 1 into g select new { Count = g.Sum(x=>x.Count) }"
				});
				Assert.NotNull(agg.GetAggregation("Test"));

				fileSystem = agg.Storage.StorageState.FileSystem;

			    await agg.DisposeAsync();
			}

			using (var agg = new AggregationEngine())
			{
				agg.Storage.StorageState.FileSystem = fileSystem;
				await agg.InitAsync();
				Assert.NotNull(agg.GetAggregation("Test"));
			}
		}

		[Fact]
		public async Task CanUpdate()
		{
			using (var agg = new AggregationEngine())
			{
				await agg.InitAsync();

				await agg.CreateAggregationAsync(new IndexDefinition
				{
					Name = "Test",
					Map = "from doc in docs select new { Count = 1}",
					Reduce = "from result in results group result by 1 into g select new { Count = g.Sum(x=>x.Count) }"
				});

				var indexDefinition = new IndexDefinition
					{
						Name = "Test",
						Map = "from doc in docs select new { Sum = 1}",
						Reduce = "from result in results group result by 1 into g select new { Sum = g.Sum(x=>x.Sum) }"
					};
				await agg.CreateAggregationAsync(indexDefinition);

				Assert.Contains(indexDefinition.Reduce, agg.GetAggregation("Test").Generator.ViewText);
			}
		}
    }
}
