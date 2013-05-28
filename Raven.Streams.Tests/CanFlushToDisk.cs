using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Json.Linq;
using Xunit;

namespace Raven.Streams.Tests
{
	public class CanFlushToDisk
	{
		[Fact]
		public void AndGetRightCount_Sequence()
		{
			using (var options = new StreamOptions())
			using (var eventStream = new EventStream(options))
			{
				for (int i = 0; i < 15; i++)
				{
					eventStream.AppendAsync(new RavenJObject {{"counter", i}}).Wait();
				}

				eventStream.FlushMemTableToFiskAsync(Etag.Empty).Wait();
				eventStream.FlushingMemTable.Wait();
				Assert.Equal(15, eventStream.EventCount);
			}
		}

		[Fact]
		public void AndGetRightCount_Parallel()
		{
			using (var options = new StreamOptions())
			using (var eventStream = new EventStream(options))
			{
				var tasks = new List<Task>();
				for (int x = 0; x < 3; x++)
				{
					for (int i = 0; i < 10; i++)
					{
						tasks.Add(Task.Factory.StartNew(() =>
						{
							for (int j = 0; j < 5; j++)
							{
								eventStream.AppendAsync(new RavenJObject { { "counter", i + "-" + j } }).Wait();
							}
						}));
					}

					foreach (var task in tasks)
					{
						while (task.IsCompleted == false && task.IsFaulted == false)
						{
							Thread.Sleep(100);
						}
					}

					eventStream.FlushMemTableToFiskAsync(Etag.Empty).Wait();

					eventStream.FlushingMemTable.Wait();
				}

				Assert.Equal(150, eventStream.EventCount);
			}
		}
	}
}