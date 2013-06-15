using System;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Json.Linq;
using Xunit;

namespace Raven.Aggregation.Tests
{
	public class DoingAggregation : IDisposable
	{
		private AggregationEngine _agg;

		[Fact]
		public async Task CanAdd()
		{
			_agg = new AggregationEngine();
			using (_agg)
			{
				await _agg.InitAsync();
				await _agg.CreateAggregationAsync(new IndexDefinition
				{
					Name = "Test",
					Map = "from doc in docs select new { Count = 1}",
					Reduce = "from result in results group result by 1 into g select new { Count = g.Sum(x=>x.Count) }"
				});

				Etag last = null;
				for (int j = 0; j < 3; j++)
				{
					for (int i = 0; i < 15; i++)
					{
						last = await _agg.AppendAsync("test", new RavenJObject { { "Item", i } });
					}

					var aggregation = _agg.GetAggregation("test");
					
					aggregation.WaitForEtag(last);
					
					var result = aggregation.AggregationResultFor("1");

					Assert.Equal(15 * (j + 1), result.Value<int>("Count"));
				}
				await _agg.DisposeAsync();
			}
		}

		public void Dispose()
		{
			throw new NotImplementedException();
		}
	}
}