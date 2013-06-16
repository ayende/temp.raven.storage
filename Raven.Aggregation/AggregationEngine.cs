using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Logging;
using Raven.Database.Linq;
using Raven.Database.Util;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;
using Raven.Storage;
using Raven.Storage.Data;
using Raven.Storage.Impl;
using Raven.Storage.Reading;
using Raven.Storage.Util;

namespace Raven.Aggregation
{
	public class AggregationEngine : IDisposable
	{
		private static readonly ILog _log = LogManager.GetCurrentClassLogger();
		private readonly ConcurrentDictionary<string, Aggregator> _aggregations
			= new ConcurrentDictionary<string, Aggregator>(StringComparer.InvariantCultureIgnoreCase);

		private readonly Storage.Storage _storage;
		private readonly string _path;
		private readonly SequentialUuidGenerator _sequentialUuidGenerator;

		private Etag _lastGeneratedEtag;
		private readonly AsyncEvent _appendEvent = new AsyncEvent();
		private volatile bool _doWork;

		public AggregationEngine(string path = null)
		{
			_path = path ?? Path.GetTempPath();
			var storageState = new StorageState(path, new StorageOptions());
			if (path == null)
				storageState.FileSystem = new InMemoryFileSystem("memory");
			_storage = new Storage.Storage(storageState);
			_sequentialUuidGenerator = new SequentialUuidGenerator
				{
					EtagBase = 1
				};
		}

		public Storage.Storage Storage
		{
			get { return _storage; }
		}

		public Etag LastGeneratedEtag
		{
			get { return _lastGeneratedEtag; }
		}

		public bool DoWork
		{
			get { return _doWork; }
		}

		public async Task InitAsync()
		{
			_doWork = true;
			await _storage.InitAsync();
			using (var it = await _storage.Reader.NewIteratorAsync(new ReadOptions()))
			{
                ReadLastEtagGenerated(it);
                ReadAllAggregations(it);
			}
			await ReadAndUpdateSystemConfig();

		}

		private void ReadLastEtagGenerated(DbIterator it)
		{
			Slice lastEventEver = "events/" + new Etag((UuidType) byte.MaxValue, long.MaxValue, long.MaxValue);
			it.Seek(lastEventEver);
			if (it.IsValid)
			{
				it.Prev();
				if (it.IsValid && it.Key.StartsWith(_eventsPrefix))
				{
					_lastGeneratedEtag = ParseEtag(it.Key);
				}
			}
		}

		private async Task ReadAndUpdateSystemConfig()
		{
			Slice configKey = "system/config";
			SystemConfig systemConfig;
			using (var stream = _storage.Reader.Read(configKey))
			{
				systemConfig = stream != null
								   ? new JsonSerializer().Deserialize<SystemConfig>(new JsonTextReader(new StreamReader(stream)))
								   : new SystemConfig();
				systemConfig.EtagBase++;
				_sequentialUuidGenerator.EtagBase = systemConfig.EtagBase;
			}

			var writeBatch = new WriteBatch();
			writeBatch.Put(configKey, SmallObjectToMemoryStream(systemConfig));
			await _storage.Writer.WriteAsync(writeBatch);
		}

		private class SystemConfig
		{
			public long EtagBase { get; set; }
		}

		private void ReadAllAggregations(DbIterator it)
		{
			Slice prefix = "aggregators/";
			it.Seek(prefix);
			while (it.IsValid)
			{
				if (it.Key.StartsWith(prefix) == false)
					break;

				string name = Encoding.UTF8.GetString(it.Key.Array, it.Key.Offset, it.Key.Count);
				using (var stream = it.CreateValueStream())
				{
					var indexDefinition =
						new JsonSerializer().Deserialize<IndexDefinition>(new JsonTextReader(new StreamReader(stream)));
					AbstractViewGenerator generator = null;
					try
					{
						var dynamicViewCompiler = new DynamicViewCompiler(indexDefinition.Name, indexDefinition,
						                                                  Path.Combine(_path, "Generators"));
						generator = dynamicViewCompiler.GenerateInstance();
					}
					catch (Exception e)
					{
						_log.WarnException("Could not create instance of aggregator " + indexDefinition.Name, e);
						// could not create generator, ignoring this and deleting the generator
						RemoveAggregation(name);
					}

					if (generator != null)
					{
						var aggregator = new Aggregator(this, indexDefinition.Name, generator);
						_aggregations.TryAdd(indexDefinition.Name, aggregator);
						Background.Work(aggregator.StartAggregation);
					}
				}

				it.Next();
			}
		}

		public void RemoveAggregation(string name)
		{
			throw new NotImplementedException();
		}

		private volatile bool _disposed;
		private static Slice _eventsPrefix = "events/";

		public void Dispose()
		{
			if (_disposed)
				return;
			ThreadPool.QueueUserWorkItem(_ => DisposeAsync());
		}

		public async Task CreateAggregationAsync(IndexDefinition indexDefinition)
		{
			var dynamicViewCompiler = new DynamicViewCompiler(indexDefinition.Name, indexDefinition, Path.Combine(_path, "Generators"));
			var generator = dynamicViewCompiler.GenerateInstance();

			var writeBatch = new WriteBatch();
			var memoryStream = SmallObjectToMemoryStream(indexDefinition);
			writeBatch.Put("aggregators/" + indexDefinition.Name, memoryStream);
			await _storage.Writer.WriteAsync(writeBatch);

			var aggregator = new Aggregator(this, indexDefinition.Name, generator);
			_aggregations.AddOrUpdate(indexDefinition.Name, aggregator, (s, viewGenerator) => aggregator);
			Background.Work(aggregator.StartAggregation);
		}

		private static MemoryStream SmallObjectToMemoryStream(object obj)
		{
			return RavenJTokenToStream(RavenJObject.FromObject(obj));
		}

		internal static MemoryStream RavenJTokenToStream(RavenJToken item)
		{
			var memoryStream = new MemoryStream();
			var streamWriter = new StreamWriter(memoryStream);
			item.WriteTo(new JsonTextWriter(streamWriter));
			streamWriter.Flush();
			memoryStream.Position = 0;
			return memoryStream;
		}

		public Aggregator GetAggregation(string name)
		{
			Aggregator value;
			_aggregations.TryGetValue(name, out value);
			return value;
		}

		public Task<Etag> AppendAsync(string topic, params RavenJObject[] items)
		{
			return AppendAsync(topic, (IEnumerable<RavenJObject>)items);
		}

		public async Task<Etag> AppendAsync(string topic, IEnumerable<RavenJObject> items)
		{
			if (items == null)
				return null;

			var batch = new WriteBatch();

			Etag etag = null;
			foreach (var item in items)
			{
				RavenJToken metadata;
				if (item.TryGetValue("@metadata", out metadata) == false)
					item["@metadata"] = metadata = new RavenJObject();
				((RavenJObject)metadata)["Raven-Entity-Name"] = topic;
				etag = GetNextEtag();
				Slice key = "events/" + etag;
				batch.Put(key, RavenJTokenToStream(item));
			}

			await _storage.Writer.WriteAsync(batch);
			_appendEvent.PulseAll();
			return etag;
		}

		private Etag GetNextEtag()
		{
			Etag etag = _sequentialUuidGenerator.CreateSequentialUuid(UuidType.DocumentTransactions);

			while (true)
			{
				Etag lastGeneratedEtag = _lastGeneratedEtag;
				if (lastGeneratedEtag != null &&
					lastGeneratedEtag.CompareTo(etag) >= 0) 
					continue;
				if (Interlocked.CompareExchange(ref _lastGeneratedEtag, etag, lastGeneratedEtag) == lastGeneratedEtag)
					break;
			}

			return etag;
		}

		public async Task<IEnumerable<EventData>> Events(Etag after)
		{
			Slice key = "events/" + after.IncrementBy(1);
			var it = await _storage.Reader.NewIteratorAsync(new ReadOptions());
			return YieldEvents(key, it);
		}

		private static IEnumerable<EventData> YieldEvents(Slice key, IIterator it)
		{
			using (it)
			{
				it.Seek(key);
				while (it.IsValid)
				{
					if (it.Key.StartsWith(_eventsPrefix) == false)
						break;

					using (var stream = it.CreateValueStream())
					{
						var token = RavenJToken.ReadFrom(new JsonTextReader(new StreamReader(stream)));
						yield return new EventData
							{
								Data = (RavenJObject)token,
								Etag = ParseEtag(it.Key)
							};
					}

					it.Next();
				}
			}
		}

		private static Etag ParseEtag(Slice key)
		{
			var str = Encoding.UTF8.GetString(key.Array, key.Offset + _eventsPrefix.Count, key.Count - _eventsPrefix.Count);
			return Etag.Parse(str);
		}

		public Task<bool> WaitForAppendAsync(Reference<int> callerState)
		{
			return _appendEvent.WaitAsync(callerState);
		}

		public async Task DisposeAsync()
		{
			if (_disposed)
				return;

			_doWork = false;
			_disposed = true;
			_appendEvent.Dispose();

			foreach (var aggregation in _aggregations.Values)
			{
				await aggregation.DisposeAsync();
			}

			_storage.Dispose();
		}
	}
}