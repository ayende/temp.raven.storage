using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Json.Linq;
using Raven.Storage.Impl;
using Xunit;

namespace Raven.Aggregation.Tests
{
	public class Events
	{
		[Fact]
		public async Task CanAppend()
		{
			using (var agg = new AggregationEngine())
			{
				await agg.InitAsync();
				for (int i = 0; i < 15; i++)
				{
					await agg.AppendAsync("test", new RavenJObject { { "Item", i } });
				}
			}
		}

		[Fact]
		public async Task AfterRestartRememberLastEtag()
		{
			FileSystem fileSystem;
			Etag lastGeneratedEtag;
			using (var agg = new AggregationEngine())
			{
				
				await agg.InitAsync();
				fileSystem = agg.Storage.StorageState.FileSystem;
				for (int i = 0; i < 15; i++)
				{
					await agg.AppendAsync("test", new RavenJObject { { "Item", i } });
				}

				Assert.NotEqual(Etag.Empty, agg.LastGeneratedEtag);

				lastGeneratedEtag = agg.LastGeneratedEtag;
				await agg.DisposeAsync();
			}

			using (var agg = new AggregationEngine())
			{
				agg.Storage.StorageState.FileSystem = fileSystem;
				await agg.InitAsync();
				Assert.Equal(lastGeneratedEtag, agg.LastGeneratedEtag);
				await agg.DisposeAsync();
			}
		}

		[Fact]
		public async Task CanAppendParallel()
		{
			using (var agg = new AggregationEngine())
			{
				await agg.InitAsync();
				var tasks = new List<Task>();
				for (int i = 0; i < 15; i++)
				{
					 tasks.Add(agg.AppendAsync("test", new RavenJObject { { "Item", i } }));
				}
				await Task.WhenAll(tasks);
				await agg.DisposeAsync();
			}
		}

		[Fact]
		public async Task CanIterate()
		{
			using (var agg = new AggregationEngine())
			{
				await agg.InitAsync();
				for (int i = 0; i < 15; i++)
				{
					await agg.AppendAsync("test", new RavenJObject { { "Val", i } });
				}

				int j = 0;
				foreach (var item in await agg.Events(Etag.Empty))
				{
					Assert.Equal(j++, item.Data.Value<int>("Val"));
					Assert.NotNull(item.Etag);
				}
				await agg.DisposeAsync();
			}
		} 
	}
}