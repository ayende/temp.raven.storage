using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Raven.Json.Linq;
using Xunit;

namespace Raven.Streams.Tests
{
	public class CanWriteData
	{
		[Fact]
		public void Sequence()
		{
			using (var options = new StreamOptions())
			using (var eventStream = new EventStream(options))
			{
				for (int i = 0; i < 5; i++)
				{
					eventStream.AppendAsync(new RavenJObject {{"counter", i}}).Wait();
				}

				Assert.Equal(5, eventStream.EventCount);
			}
		}

		[Fact]
		public void Parallel()
		{
			using (var options = new StreamOptions())
			using (var eventStream = new EventStream(options))
			{
				var tasks = new List<Task>();
				for (int i = 0; i < 10; i++)
				{
					tasks.Add(Task.Factory.StartNew(() =>
						{
							for (int j = 0; j < 5; j++)
							{
								eventStream.AppendAsync(new RavenJObject { { "counter", i +"-" + j } }).Wait();
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

				Assert.Equal(50, eventStream.EventCount);
			}
		}

	}
}
