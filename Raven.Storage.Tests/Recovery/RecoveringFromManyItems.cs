using System.IO;
using System.Threading.Tasks;
using Raven.Storage.Impl;
using Xunit;

namespace Raven.Storage.Tests.Recovery
{
	public class RecoveringFromManyItems : StorageTestBase
	{
		[Fact]
		public async Task CanOpenAndCloseWithUpdate()
		{
			FileSystem fileSystem;
			using (var db = await NewStorageAsync())
			{
				fileSystem = db.StorageState.FileSystem;

				db.Put("aggregators/Test", "{\"Name\":\"Test\",\"LockMode\":\"Unlock\",\"Map\":\"from doc in docs select new { Count = 1}\",\"Maps\":[\"from doc in docs select new { Count = 1}\"],\"Reduce\":\"from result in results group result by 1 into g select new { Count = g.Sum(x=>x.Count) }\",\"TransformResults\":null,\"IsMapReduce\":true,\"IsCompiled\":false,\"Stores\":{},\"Indexes\":{},\"SortOptions\":{},\"Analyzers\":{},\"Fields\":[],\"Suggestions\":{},\"TermVectors\":{},\"SpatialIndexes\":{},\"Type\":\"MapReduce\"}");
				db.Put("system/config", "{\"EtagBase\":1}");

				db.Put("events/03000000-0000-0001-0000-00000000002D", "{\"Item\":14,\"@metadata\":{\"Raven-Entity-Name\":\"test\"}}");
				db.Put("events/03000000-0000-0001-0000-00000000002C", "{\"Item\":13,\"@metadata\":{\"Raven-Entity-Name\":\"test\"}}");
				db.Put("events/03000000-0000-0001-0000-00000000002B", "{\"Item\":12,\"@metadata\":{\"Raven-Entity-Name\":\"test\"}}");
				db.Put("events/03000000-0000-0001-0000-00000000002A", "{\"Item\":11,\"@metadata\":{\"Raven-Entity-Name\":\"test\"}}");
				db.Put("events/03000000-0000-0001-0000-000000000029", "{\"Item\":10,\"@metadata\":{\"Raven-Entity-Name\":\"test\"}}");
				db.Put("events/03000000-0000-0001-0000-000000000028", "{\"Item\":9,\"@metadata\":{\"Raven-Entity-Name\":\"test\"}}");
				db.Put("events/03000000-0000-0001-0000-000000000027", "{\"Item\":8,\"@metadata\":{\"Raven-Entity-Name\":\"test\"}}");
				db.Put("events/03000000-0000-0001-0000-000000000026", "{\"Item\":7,\"@metadata\":{\"Raven-Entity-Name\":\"test\"}}");
				db.Put("events/03000000-0000-0001-0000-000000000025", "{\"Item\":6,\"@metadata\":{\"Raven-Entity-Name\":\"test\"}}");
				db.Put("events/03000000-0000-0001-0000-000000000024", "{\"Item\":5,\"@metadata\":{\"Raven-Entity-Name\":\"test\"}}");
				db.Put("events/03000000-0000-0001-0000-000000000023", "{\"Item\":4,\"@metadata\":{\"Raven-Entity-Name\":\"test\"}}");
				db.Put("events/03000000-0000-0001-0000-000000000022", "{\"Item\":3,\"@metadata\":{\"Raven-Entity-Name\":\"test\"}}");
				db.Put("events/03000000-0000-0001-0000-000000000021", "{\"Item\":2,\"@metadata\":{\"Raven-Entity-Name\":\"test\"}}");
				db.Put("events/03000000-0000-0001-0000-000000000020", "{\"Item\":1,\"@metadata\":{\"Raven-Entity-Name\":\"test\"}}");
				db.Put("events/03000000-0000-0001-0000-00000000001F", "{\"Item\":0,\"@metadata\":{\"Raven-Entity-Name\":\"test\"}}");
				db.Put("events/03000000-0000-0001-0000-00000000001E", "{\"Item\":14,\"@metadata\":{\"Raven-Entity-Name\":\"test\"}}");
				db.Put("events/03000000-0000-0001-0000-00000000001D", "{\"Item\":13,\"@metadata\":{\"Raven-Entity-Name\":\"test\"}}");
				db.Put("events/03000000-0000-0001-0000-00000000001C", "{\"Item\":12,\"@metadata\":{\"Raven-Entity-Name\":\"test\"}}");
				db.Put("events/03000000-0000-0001-0000-00000000001B", "{\"Item\":11,\"@metadata\":{\"Raven-Entity-Name\":\"test\"}}");
				db.Put("events/03000000-0000-0001-0000-00000000001A", "{\"Item\":10,\"@metadata\":{\"Raven-Entity-Name\":\"test\"}}");
				db.Put("events/03000000-0000-0001-0000-000000000019", "{\"Item\":9,\"@metadata\":{\"Raven-Entity-Name\":\"test\"}}");
				db.Put("events/03000000-0000-0001-0000-000000000018", "{\"Item\":8,\"@metadata\":{\"Raven-Entity-Name\":\"test\"}}");
				db.Put("events/03000000-0000-0001-0000-000000000017", "{\"Item\":7,\"@metadata\":{\"Raven-Entity-Name\":\"test\"}}");
				db.Put("events/03000000-0000-0001-0000-000000000016", "{\"Item\":6,\"@metadata\":{\"Raven-Entity-Name\":\"test\"}}");
				db.Put("events/03000000-0000-0001-0000-000000000015", "{\"Item\":5,\"@metadata\":{\"Raven-Entity-Name\":\"test\"}}");
				db.Put("events/03000000-0000-0001-0000-000000000014", "{\"Item\":4,\"@metadata\":{\"Raven-Entity-Name\":\"test\"}}");
				db.Put("events/03000000-0000-0001-0000-000000000013", "{\"Item\":3,\"@metadata\":{\"Raven-Entity-Name\":\"test\"}}");
				db.Put("events/03000000-0000-0001-0000-000000000012", "{\"Item\":2,\"@metadata\":{\"Raven-Entity-Name\":\"test\"}}");
				db.Put("events/03000000-0000-0001-0000-000000000011", "{\"Item\":1,\"@metadata\":{\"Raven-Entity-Name\":\"test\"}}");
				db.Put("events/03000000-0000-0001-0000-000000000010", "{\"Item\":0,\"@metadata\":{\"Raven-Entity-Name\":\"test\"}}");
				db.Put("events/03000000-0000-0001-0000-00000000000F", "{\"Item\":14,\"@metadata\":{\"Raven-Entity-Name\":\"test\"}}");
				db.Put("events/03000000-0000-0001-0000-00000000000E", "{\"Item\":13,\"@metadata\":{\"Raven-Entity-Name\":\"test\"}}");
				db.Put("events/03000000-0000-0001-0000-00000000000D", "{\"Item\":12,\"@metadata\":{\"Raven-Entity-Name\":\"test\"}}");
				db.Put("events/03000000-0000-0001-0000-00000000000C", "{\"Item\":11,\"@metadata\":{\"Raven-Entity-Name\":\"test\"}}");
				db.Put("events/03000000-0000-0001-0000-00000000000B", "{\"Item\":10,\"@metadata\":{\"Raven-Entity-Name\":\"test\"}}");
				db.Put("events/03000000-0000-0001-0000-00000000000A", "{\"Item\":9,\"@metadata\":{\"Raven-Entity-Name\":\"test\"}}");
				db.Put("events/03000000-0000-0001-0000-000000000009", "{\"Item\":8,\"@metadata\":{\"Raven-Entity-Name\":\"test\"}}");
				db.Put("events/03000000-0000-0001-0000-000000000008", "{\"Item\":7,\"@metadata\":{\"Raven-Entity-Name\":\"test\"}}");
				db.Put("events/03000000-0000-0001-0000-000000000007", "{\"Item\":6,\"@metadata\":{\"Raven-Entity-Name\":\"test\"}}");
				db.Put("events/03000000-0000-0001-0000-000000000006", "{\"Item\":5,\"@metadata\":{\"Raven-Entity-Name\":\"test\"}}");
				db.Put("events/03000000-0000-0001-0000-000000000005", "{\"Item\":4,\"@metadata\":{\"Raven-Entity-Name\":\"test\"}}");
				db.Put("events/03000000-0000-0001-0000-000000000004", "{\"Item\":3,\"@metadata\":{\"Raven-Entity-Name\":\"test\"}}");
				db.Put("events/03000000-0000-0001-0000-000000000003", "{\"Item\":2,\"@metadata\":{\"Raven-Entity-Name\":\"test\"}}");
				db.Put("events/03000000-0000-0001-0000-000000000002", "{\"Item\":1,\"@metadata\":{\"Raven-Entity-Name\":\"test\"}}");
				db.Put("events/03000000-0000-0001-0000-000000000001", "{\"Item\":0,\"@metadata\":{\"Raven-Entity-Name\":\"test\"}}");


				db.Put("status/Test", "{\"@etag\":\"03000000-0000-0001-0000-000000000004\"}");
				db.Put("status/Test", "{\"@etag\":\"03000000-0000-0001-0000-00000000000F\"}");
				db.Put("status/Test", "{\"@etag\":\"03000000-0000-0001-0000-000000000010\"}");
				db.Put("status/Test", "{\"@etag\":\"03000000-0000-0001-0000-000000000011\"}");
				db.Put("status/Test", "{\"@etag\":\"03000000-0000-0001-0000-000000000012\"}");
				db.Put("status/Test", "{\"@etag\":\"03000000-0000-0001-0000-000000000013\"}");
				db.Put("status/Test", "{\"@etag\":\"03000000-0000-0001-0000-000000000014\"}");
				db.Put("status/Test", "{\"@etag\":\"03000000-0000-0001-0000-000000000015\"}");
				db.Put("status/Test", "{\"@etag\":\"03000000-0000-0001-0000-000000000016\"}");
				db.Put("status/Test", "{\"@etag\":\"03000000-0000-0001-0000-000000000017\"}");
				db.Put("status/Test", "{\"@etag\":\"03000000-0000-0001-0000-000000000018\"}");
				db.Put("status/Test", "{\"@etag\":\"03000000-0000-0001-0000-000000000019\"}");
				db.Put("status/Test", "{\"@etag\":\"03000000-0000-0001-0000-00000000001A\"}");
				db.Put("status/Test", "{\"@etag\":\"03000000-0000-0001-0000-00000000001B\"}");
				db.Put("status/Test", "{\"@etag\":\"03000000-0000-0001-0000-00000000001C\"}");
				db.Put("status/Test", "{\"@etag\":\"03000000-0000-0001-0000-00000000001D\"}");
				db.Put("status/Test", "{\"@etag\":\"03000000-0000-0001-0000-00000000001E\"}");
				db.Put("status/Test", "{\"@etag\":\"03000000-0000-0001-0000-00000000001F\"}");
				db.Put("status/Test", "{\"@etag\":\"03000000-0000-0001-0000-000000000020\"}");
				db.Put("status/Test", "{\"@etag\":\"03000000-0000-0001-0000-000000000021\"}");
				db.Put("status/Test", "{\"@etag\":\"03000000-0000-0001-0000-000000000022\"}");
				db.Put("status/Test", "{\"@etag\":\"03000000-0000-0001-0000-000000000023\"}");
				db.Put("status/Test", "{\"@etag\":\"03000000-0000-0001-0000-000000000024\"}");
				db.Put("status/Test", "{\"@etag\":\"03000000-0000-0001-0000-000000000025\"}");
				db.Put("status/Test", "{\"@etag\":\"03000000-0000-0001-0000-000000000026\"}");
				db.Put("status/Test", "{\"@etag\":\"03000000-0000-0001-0000-000000000027\"}");
				db.Put("status/Test", "{\"@etag\":\"03000000-0000-0001-0000-000000000028\"}");
				db.Put("status/Test", "{\"@etag\":\"03000000-0000-0001-0000-000000000029\"}");
				db.Put("status/Test", "{\"@etag\":\"03000000-0000-0001-0000-00000000002A\"}");
				db.Put("status/Test", "{\"@etag\":\"03000000-0000-0001-0000-00000000002C\"}");
				db.Put("status/Test", "{\"@etag\":\"03000000-0000-0001-0000-00000000002D\"}");
				db.Put("results/Test/1", "{\"Count\":4.0}");
				db.Put("results/Test/1", "{\"Count\":15.0}");
				db.Put("results/Test/1", "{\"Count\":16.0}");
				db.Put("results/Test/1", "{\"Count\":17.0}");
				db.Put("results/Test/1", "{\"Count\":18.0}");
				db.Put("results/Test/1", "{\"Count\":19.0}");
				db.Put("results/Test/1", "{\"Count\":20.0}");
				db.Put("results/Test/1", "{\"Count\":21.0}");
				db.Put("results/Test/1", "{\"Count\":22.0}");
				db.Put("results/Test/1", "{\"Count\":23.0}");
				db.Put("results/Test/1", "{\"Count\":24.0}");
				db.Put("results/Test/1", "{\"Count\":25.0}");
				db.Put("results/Test/1", "{\"Count\":26.0}");
				db.Put("results/Test/1", "{\"Count\":27.0}");
				db.Put("results/Test/1", "{\"Count\":28.0}");
				db.Put("results/Test/1", "{\"Count\":29.0}");
				db.Put("results/Test/1", "{\"Count\":30.0}");
				db.Put("results/Test/1", "{\"Count\":31.0}");
				db.Put("results/Test/1", "{\"Count\":32.0}");
				db.Put("results/Test/1", "{\"Count\":33.0}");
				db.Put("results/Test/1", "{\"Count\":34.0}");
				db.Put("results/Test/1", "{\"Count\":35.0}");
				db.Put("results/Test/1", "{\"Count\":36.0}");
				db.Put("results/Test/1", "{\"Count\":37.0}");
				db.Put("results/Test/1", "{\"Count\":38.0}");
				db.Put("results/Test/1", "{\"Count\":39.0}");
				db.Put("results/Test/1", "{\"Count\":40.0}");
				db.Put("results/Test/1", "{\"Count\":41.0}");
				db.Put("results/Test/1", "{\"Count\":42.0}");
				db.Put("results/Test/1", "{\"Count\":44.0}");
				db.Put("results/Test/1", "{\"Count\":45.0}");
				

			}

			using (var db = await NewStorageAsync(fileSystem: fileSystem))
			{
				using (var s = db.Reader.Read("results/Test/1"))
				{
					var readToEnd = new StreamReader(s).ReadToEnd();
					Assert.Equal("{\"Count\":45.0}", readToEnd);
				}
			}
			
		}
	}
}