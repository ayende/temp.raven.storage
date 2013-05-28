using System;
using System.IO;
using Raven.Abstractions.Data;
using Raven.Json.Linq;
using Xunit;

namespace Raven.Streams.Tests
{
	public class CanWriteToActualDisk : IDisposable
	{
		[Fact]
		public void AndGetRightCount_Sequence()
		{
			using (var options = new StreamOptions
				{
					Storage = new FileSystemLowLevelStorage("data")
				})
			using (var eventStream = new EventStream(options))
			{
				for (int i = 0; i < 15; i++)
				{
					eventStream.AppendAsync(new RavenJObject { { "counter", i } }).Wait();
				}

				eventStream.FlushMemTableToFiskAsync(Etag.Empty).Wait();
				eventStream.FlushingMemTable.Wait();
				Assert.Equal(15, eventStream.EventCount);
			}
		}

		[Fact]
		public void CanRestart()
		{
			using (var options = new StreamOptions
			{
				Storage = new FileSystemLowLevelStorage("data")
			})
			{
				using (var eventStream = new EventStream(options))
				{
					for (int i = 0; i < 15; i++)
					{
						eventStream.AppendAsync(new RavenJObject { { "counter", i } }).Wait();
					}

					eventStream.FlushMemTableToFiskAsync(Etag.Empty).Wait();
					eventStream.FlushingMemTable.Wait();
				}

				using (var eventStream = new EventStream(options))
				{
					Assert.Equal(15, eventStream.EventCount);

					int x = 0;
					foreach (var it in eventStream.ReadFrom(Etag.Empty))
					{
						Assert.Equal(x++, it.Value<int>("counter"));
					}
				}
			}
		}

		public void Dispose()
		{
			Directory.Delete("data", true);
		}
	}
}