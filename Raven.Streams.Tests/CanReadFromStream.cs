using System;
using System.IO;
using Raven.Abstractions.Data;
using Raven.Imports.Newtonsoft.Json.Bson;
using Raven.Json.Linq;
using Xunit;
using System.Linq;

namespace Raven.Streams.Tests
{
	public class CanReadFromStream
	{
		[Fact]
		public void AndReadContentsProperly()
		{
			using (var options = new StreamOptions())
			using (var eventStream = new EventStream(options))
			{
				for (int i = 0; i < 15; i++)
				{
					eventStream.AppendAsync(new RavenJObject { { "counter", i } }).Wait();
				}

				int x = 0;
				foreach (var item in eventStream.ReadFrom(Etag.Empty))
				{
					var value = item.Value<int>("counter");
					Assert.Equal(x++, value);
				}
			}
		}

		[Fact]
		public void AndGetRightCount_Sequence()
		{
			using (var options = new StreamOptions())
			using (var eventStream = new EventStream(options))
			{
				for (int i = 0; i < 15; i++)
				{
					eventStream.AppendAsync(new RavenJObject { { "counter", i } }).Wait();
				}

				var list = eventStream.ReadFrom(Etag.Empty).ToList();
				Assert.Equal(15, list.Count);
			}
		} 
	}
}