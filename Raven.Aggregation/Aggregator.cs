using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Linq;
using Raven.Database.Linq;
using System.Linq;
using Raven.Imports.Newtonsoft.Json;
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Json.Linq;
using Raven.Storage;
using Raven.Storage.Data;
using Raven.Storage.Impl;

namespace Raven.Aggregation
{
	public class Aggregator : IDisposable
	{
		private readonly AggregationEngine _aggregationEngine;
		private readonly string _name;
		private readonly AbstractViewGenerator _generator;
		private object _lastAggregatedEtag;
		private readonly LruCache<string, RavenJToken> _cache = new LruCache<string, RavenJToken>(2048, StringComparer.InvariantCultureIgnoreCase);
		private Task _aggregationTask;
		private readonly AsyncEvent _aggregationCompleted = new AsyncEvent();
		private readonly Reference<int> _appendEventState = new Reference<int>();
		private readonly TaskCompletionSource<object> _disposedCompletionSource = new TaskCompletionSource<object>();
		private volatile bool _disposed;
	    private readonly Slice _aggStat;


	    public Aggregator(AggregationEngine aggregationEngine, string name, AbstractViewGenerator generator)
		{
			_aggregationEngine = aggregationEngine;
			_name = name;
			_generator = generator;

		    _aggStat = "status/" + _name;

	        using (var stream = aggregationEngine.Storage.Reader.Read(_aggStat))
	        {
	            if (stream == null)
	            {
                    _lastAggregatedEtag = Etag.Empty;
	                return;
	            }
	            var status = RavenJObject.Load(new JsonTextReader(new StreamReader(stream)));
	            _lastAggregatedEtag = Etag.Parse(status.Value<string>("@etag"));
	        }
		}

		public AbstractViewGenerator Generator
		{
			get { return _generator; }
		}

		public void StartAggregation()
		{
			_aggregationTask = AggregateAsync();
		}

		private async Task AggregateAsync()
		{
			var lastAggregatedEtag = (Etag) _lastAggregatedEtag;
			while (_aggregationEngine.DoWork)
			{
				var eventDatas = (await _aggregationEngine.Events(lastAggregatedEtag)).Take(1024)
					.ToArray();
				if (eventDatas.Length == 0)
				{
					await _aggregationEngine.WaitForAppendAsync(_appendEventState);
					continue;
				}
				var items = eventDatas.Select(x => new DynamicJsonObject(x.Data)).ToArray();
				var results = _generator.MapDefinitions
					.SelectMany(indexingFunc => indexingFunc(items))
					.ToList();

				var groupedByReduceKey = results.GroupBy(x =>
					{
						var reduceKey = _generator.GroupByExtraction(x);
						if (reduceKey == null)
							return "@null";
						var ravenJToken = RavenJToken.FromObject(reduceKey);
						if (ravenJToken.Type == JTokenType.String)
							return ravenJToken.Value<string>();
						return ravenJToken.ToString(Formatting.None);
					})
						.ToArray();

				var writeBatch = new WriteBatch();
				foreach (var grouping in groupedByReduceKey)
				{
					Slice key = "results/" + _name + "/" + grouping.Key;
					RavenJToken currentStatus;
					if (_cache.TryGet(grouping.Key, out currentStatus) == false)
					{
						using (var stream = _aggregationEngine.Storage.Reader.Read(key))
						{
							if (stream != null)
							{
								currentStatus = RavenJToken.ReadFrom(new JsonTextReader(new StreamReader(stream)));
							}
						}
					}

					IEnumerable<dynamic> groupedResults = grouping;
					if (currentStatus != null)
					{
						switch (currentStatus.Type)
						{
							case JTokenType.Array:
								groupedResults = groupedResults.Concat(((RavenJArray) currentStatus).Select(x => new DynamicJsonObject((RavenJObject)x)));
								break;
							case JTokenType.Object:
								groupedResults = grouping.Concat(new dynamic[] {new DynamicJsonObject((RavenJObject)currentStatus)});
								break;
						}
					}

					var reduceResults = _generator.ReduceDefinition(groupedResults).ToArray();

					RavenJToken finalResult;
					switch (reduceResults.Length)
					{
						case 0:
							throw new InvalidOperationException("How did this happen? No results were gotten from the reduce!");
						case 1:
							finalResult = RavenJObject.FromObject(reduceResults[0]);
							break;
						default:
							finalResult = new RavenJArray(reduceResults.Select(x => RavenJObject.FromObject(x)));
							break;
					}
                    finalResult.EnsureCannotBeChangeAndEnableSnapshotting();
					_cache.Set(grouping.Key, finalResult);
					writeBatch.Put(key, AggregationEngine.RavenJTokenToStream(finalResult));
				}

				lastAggregatedEtag = eventDatas.Last().Etag;
				
				var status = new RavenJObject { { "@etag", lastAggregatedEtag.ToString() } };
			    writeBatch.Put(_aggStat, AggregationEngine.RavenJTokenToStream(status));
				await _aggregationEngine.Storage.Writer.WriteAsync(writeBatch);

				Thread.VolatileWrite(ref _lastAggregatedEtag, lastAggregatedEtag);

				_aggregationCompleted.PulseAll();
			}
		}

		public void Dispose()
		{
			if (_disposed)
				return;
			ThreadPool.QueueUserWorkItem(state => DisposeAsync());
		}

		public async Task WaitForEtagAsync(Etag etag)
		{
			if (etag == null)
				return;
			var callerState = new Reference<int>();
			while (true)
			{
                var lastAggregatedEtag = LastAggregatedEtag;
				if (etag.CompareTo(lastAggregatedEtag) <= 0)
					return;
				await _aggregationCompleted.WaitAsync(callerState);
			}
		}

		public RavenJToken AggregationResultFor(string item)
		{
			RavenJToken value;
			if (_cache.TryGet(item, out value))
				return value.CreateSnapshot();
			Slice key = "results/" + _name + "/" + item;
			using (var stream = _aggregationEngine.Storage.Reader.Read(key))
			{
				if (stream != null)
				{
					value = RavenJToken.ReadFrom(new JsonTextReader(new StreamReader(stream)));
                    value.EnsureCannotBeChangeAndEnableSnapshotting();
					_cache.Set(item, value);
				    return value.CreateSnapshot();
				}
			    return null;
			}
		}

	    public Etag LastAggregatedEtag
	    {
	        get { return (Etag)Thread.VolatileRead(ref _lastAggregatedEtag); }
	    }

	    public async Task DisposeAsync()
		{
			if (_disposed)
				return;
			_disposed = true;
			_disposedCompletionSource.SetResult(null);
			_aggregationCompleted.Dispose();
			if (_aggregationTask != null)
				await _aggregationTask;
		}
	}
}