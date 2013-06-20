using Raven.Abstractions.Data;
using Raven.Json.Linq;

namespace Raven.Aggregation
{
	public class EventData
	{
		public Etag Etag;
		public RavenJObject Data;
	}

	public class ReductionData
	{
		public RavenJToken Data;
		public string ReduceKey;
	}
}