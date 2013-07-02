using System;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Json.Linq;
using Raven.Storage.Impl;
using Xunit;
using System.Linq;

namespace Raven.Aggregation.Tests
{
	public class DoingAggregation : IDisposable
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

				Etag last = null;
				for (int j = 0; j < 3; j++)
				{
					for (int i = 0; i < 15; i++)
					{
						last = await agg.AppendAsync("test", new RavenJObject { { "Item", i } });
					}

					var aggregation = agg.GetAggregation("test");

					await aggregation.WaitForEtagAsync(last);


					var eventDatas = agg.Events(Etag.Empty).ToList();
					Assert.Equal(15 * (j + 1), eventDatas.Count);
					
					var result = aggregation.AggregationResultFor("1");
					Assert.Equal(eventDatas.Count, result.Value<int>("Count"));
				}
				await agg.DisposeAsync();
			}
		}

		[Fact]
		public async Task WillRememberAfterRestart()
		{
			FileSystem fs;
			Etag last;
			using (var agg = new AggregationEngine())
			{
				await agg.InitAsync();
				fs = agg.Storage.StorageState.FileSystem;

				await agg.CreateAggregationAsync(new IndexDefinition
				{
					Name = "Test",
					Map = "from doc in docs select new { Count = 1}",
					Reduce = "from result in results group result by 1 into g select new { Count = g.Sum(x=>x.Count) }"
				});

				var aggregation = agg.GetAggregation("test");
				for (int j = 0; j < 3; j++)
				{
					Etag lastWrite = null;
					for (int i = 0; i < 15; i++)
					{
						lastWrite = await agg.AppendAsync("test", new RavenJObject { { "Item", i } });
					}


					await aggregation.WaitForEtagAsync(lastWrite);

					var result = aggregation.AggregationResultFor("1");

					Assert.Equal(15 * (j + 1), result.Value<int>("Count"));
				}

				last = aggregation.LastAggregatedEtag;
				await agg.DisposeAsync();
			}

			using (var agg = new AggregationEngine())
			{
				agg.Storage.StorageState.FileSystem = fs;
				await agg.InitAsync();

				var aggregation = agg.GetAggregation("test");
				var result = aggregation.AggregationResultFor("1");

				Assert.Equal(last, aggregation.LastAggregatedEtag);
				Assert.Equal(45, result.Value<int>("Count"));

				await agg.DisposeAsync();
			}

		}

		public void Dispose()
		{
		}
	}
}