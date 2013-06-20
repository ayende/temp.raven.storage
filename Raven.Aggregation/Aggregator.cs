using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Linq;
using Raven.Abstractions.Logging;
using Raven.Aggregation.TEMP;
using Raven.Database.Linq;
using System.Linq;
using Raven.Imports.Newtonsoft.Json;
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Json.Linq;
using Raven.Storage;
using Raven.Storage.Data;
using Raven.Storage.Impl;
using Raven.Storage.Reading;

namespace Raven.Aggregation
{
	public class Aggregator : IDisposable
	{
		private ILog Log = LogManager.GetCurrentClassLogger();

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
		private AggregateException _aggregateException;


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
			_aggregationTask = AggregateAsync()
				.ContinueWith(task =>
					{
						if (!task.IsFaulted) 
							return;
						Log.ErrorException("An error occured when running background aggregation for " + _name + ", the aggregation has been DISABLED", task.Exception);
						_aggregateException = task.Exception;
					});
		}

		private async Task AggregateAsync()
		{
			var lastAggregatedEtag = (Etag) _lastAggregatedEtag;
			while (_aggregationEngine.CancellationToken.IsCancellationRequested == false)
			{
				var eventDatas = _aggregationEngine.Events(lastAggregatedEtag).Take(1024)
					.ToArray();
				if (eventDatas.Length == 0)
				{
					await _aggregationEngine.WaitForAppendAsync(_appendEventState);
					continue;
				}
				var items = eventDatas.Select(x => new DynamicJsonObject(x.Data)).ToArray();

				var writeBatch = new WriteBatch();

				var groupedByReduceKey = ExecuteMaps(items);
				ExecuteReduce(groupedByReduceKey, writeBatch);
			
				lastAggregatedEtag = eventDatas.Last().Etag;
				
				var status = new RavenJObject { { "@etag", lastAggregatedEtag.ToString() } };
			    writeBatch.Put(_aggStat, AggregationEngine.RavenJTokenToStream(status));
				await _aggregationEngine.Storage.Writer.WriteAsync(writeBatch);

				Thread.VolatileWrite(ref _lastAggregatedEtag, lastAggregatedEtag);

				_aggregationCompleted.PulseAll();
			}
		}

		private void ExecuteReduce(IEnumerable<IGrouping<dynamic, object>> groupedByReduceKey, WriteBatch writeBatch)
		{
			foreach (var grouping in groupedByReduceKey)
			{
				string reduceKey = grouping.Key;
				Slice key = "results/" + _name + "/" + reduceKey;
				var groupedResults = GetItemsToReduce(reduceKey, key, grouping);

				var robustEnumerator = new RobustEnumerator2(_aggregationEngine.CancellationToken, 50)
					{
						OnError = (exception, o) =>
						          Log.WarnException("Could not process reduce for aggregation " + _name + Environment.NewLine + o,
						                            exception)
					};

				var reduceResults =
					robustEnumerator.RobustEnumeration(groupedResults.GetEnumerator(), _generator.ReduceDefinition).ToArray();

				RavenJToken finalResult;
				switch (reduceResults.Length)
				{
					case 0:
						Log.Warn("FLYING PIGS!!! Could not find any results for a reduce on key {0} for aggregator {1}. Should not happen", reduceKey, _name);
						finalResult = new RavenJObject {{"Error", "Invalid reduce result was generated"}};
						break;
					case 1:
						finalResult = RavenJObject.FromObject(reduceResults[0]);
						break;
					default:
						finalResult = new RavenJArray(reduceResults.Select(RavenJObject.FromObject));
						break;
				}
				finalResult.EnsureCannotBeChangeAndEnableSnapshotting();
				_cache.Set(reduceKey, finalResult);
				writeBatch.Put(key, AggregationEngine.RavenJTokenToStream(finalResult));
			}
		}

		private IEnumerable<dynamic> GetItemsToReduce(dynamic reduceKey, Slice key, IGrouping<dynamic, object> grouping)
		{
			RavenJToken currentStatus;
			if (_cache.TryGet(reduceKey, out currentStatus) == false)
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
						groupedResults =
							groupedResults.Concat(((RavenJArray) currentStatus).Select(x => new DynamicJsonObject((RavenJObject) x)));
						break;
					case JTokenType.Object:
						groupedResults = grouping.Concat(new dynamic[] {new DynamicJsonObject((RavenJObject) currentStatus)});
						break;
				}
			}
			return groupedResults;
		}

		private IGrouping<dynamic, object>[] ExecuteMaps(IEnumerable<object> items)
		{
			var robustEnumerator = new RobustEnumerator2(_aggregationEngine.CancellationToken, 50)
				{
					OnError = (exception, o) => 
						Log.WarnException("Could not process maps event for aggregator: " + _name + Environment.NewLine + o, exception)
				};

			var results = robustEnumerator
				.RobustEnumeration(items.GetEnumerator(), _generator.MapDefinitions)
			    .ToList();

			var reduced = robustEnumerator.RobustEnumeration(results.GetEnumerator(), _generator.ReduceDefinition);

			var groupedByReduceKey = reduced.GroupBy(x =>
				{
					var reduceKey = _generator.GroupByExtraction(x);
					if (reduceKey == null)
						return "@null";
					var s = reduceKey as string;
					if (s != null)
						return s;
					var ravenJToken = RavenJToken.FromObject(reduceKey);
					if (ravenJToken.Type == JTokenType.String)
						return ravenJToken.Value<string>();
					return ravenJToken.ToString(Formatting.None);
				})
			                                .ToArray();
			return groupedByReduceKey;
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
			if (_aggregateException != null)
				throw new AggregateException(_aggregateException.InnerExceptions);

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

		public IEnumerable<ReductionData> AggregationResults()
		{
			using (var it = _aggregationEngine.Storage.Reader.NewIterator(new ReadOptions()))
			{
				Slice prefix = "results/" + _name + "/";
				it.Seek(prefix);
				while(it.WithPrefix(prefix))
				{
					using (var stream = it.CreateValueStream())
					{
						var ravenJToken = RavenJToken.ReadFrom(new JsonTextReader(new StreamReader(stream)));
						yield return new ReductionData
							{
								Data = ravenJToken,
								ReduceKey = it.Key.ToString()
							};
					}
					it.Next();
				}
			}
		}
	}
}