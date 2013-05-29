using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Json;
using Raven.Imports.Newtonsoft.Json.Bson;
using Raven.Json.Linq;
using Raven.Storage.Building;
using Raven.Storage.Data;
using Raven.Storage.Impl;
using Raven.Storage.Impl.Streams;
using Raven.Storage.Memtable;
using Raven.Storage.Reading;
using Raven.Storage.Util;

namespace Raven.Streams
{
	public class EventStream : IDisposable
	{
		private volatile bool disposed;
		private readonly StreamOptions _options;
		private MemRange _memTable;
		private MemRange _immutableMemTable;
		private Task _flushingMemTable;
		private int _currentBufferSize;
		private readonly AsyncLock _asyncLock = new AsyncLock();
		private readonly ConcurrentQueue<PendingWrite> _pending = new ConcurrentQueue<PendingWrite>();
		private readonly AsyncMonitor _writeCompletedEvent = new AsyncMonitor();
		private readonly object _metadataLocker = new object();

		private readonly ConcurrentDictionary<string, Lazy<Table>> _tables =
			new ConcurrentDictionary<string, Lazy<Table>>(StringComparer.InvariantCultureIgnoreCase);

		private class PendingWrite
		{
			private readonly Stream _stream;
			private readonly Etag _etag;
			public readonly TaskCompletionSource<object> Result = new TaskCompletionSource<object>();

			public PendingWrite(Stream stream, Etag etag)
			{
				_stream = stream;
				_etag = etag;
			}

			public Etag Etag
			{
				get { return _etag; }
			}

			public Stream Stream
			{
				get { return _stream; }
			}

			public bool Done()
			{
				var task = Result.Task;
				if (task.IsCompleted)
					return true;
				if (task.IsCanceled || task.IsFaulted)
					task.Wait(); // throws
				return false;
			}
		}

		public EventStream(StreamOptions options)
		{
			_options = options;
			_options.Initialize();
			_currentBufferSize = _options.InitialInMemoryBuffer;
			long log;
			_memTable = new MemRange
				{
					MemTable = new MemTable(_currentBufferSize, options.Comparator, _options.BufferPool),
					Start = EtagToSlice(Etag.Empty),
					Log = new LogWriter(_options.CreateNewLogFile(out log), _options.BufferPool),
					LogNumber = log
				};
		}

		public int EventCount
		{
			get
			{
				lock (_metadataLocker)
				{
					var count = _memTable.MemTable.Count;
					var immutableMemTable = _immutableMemTable;
					if (immutableMemTable != null)
						count += immutableMemTable.MemTable.Count;
					count += _options.Status.Ranges.Sum(range => range.Count);
					return count;
				}
			}
		}

		public IEnumerable<RavenJObject> ReadFrom(Etag start)
		{
			if (disposed)
				throw new ObjectDisposedException("EventStream");

			var target = EtagToSlice(start);
			var comparator = _options.Comparator;

			var reference = new Reference<Slice> { Value = target };

			var memTables = new[] { _immutableMemTable, _memTable };
			for (var i = 1; i >= 0; i--)
			{
				if (memTables[i] == null)
					continue;
				if (comparator.Compare(reference.Value, memTables[i].Start) < 0)
					continue;
				for (var j = i; j < 2; j++)
				{
					foreach (var p in YieldMemTableContents(memTables[j].MemTable, reference))
						yield return p;
				}
				yield break;
			}

			var ranges = _options.Status.Ranges;
			for (var i = ranges.Count - 1; i >= 0; i--)
			{
				if (comparator.Compare(reference.Value, ranges[i].Start) < 0)
					continue;
				for (var j = i; j < ranges.Count; j++)
				{
					foreach (var p in YieldSstContents(GetTableFor(ranges[i].Name), reference))
						yield return p;
				}
				foreach (var memTable in memTables)
				{
					if (memTable == null)
						continue;
					foreach (var p in YieldMemTableContents(memTable.MemTable, reference))
						yield return p;
				}
			}
		}

		private IEnumerable<RavenJObject> YieldSstContents(Table table, Reference<Slice> reference)
		{
			using (var iterator = table.CreateIterator(new ReadOptions()))
			{
				iterator.Seek(reference.Value);
				while (iterator.IsValid)
				{
					reference.Value = iterator.Key;
					using (var stream = iterator.CreateValueStream())
					{
						yield return RavenJObject.Load(new BsonReader(stream));
					}
					iterator.Next();
				}
			}
		}

		private Table GetTableFor(string fileName)
		{
			var lazy = new Lazy<Table>(() => new Table(_options, _options.Storage.FileData(fileName)));
			return _tables.GetOrAdd(fileName, s => lazy).Value;
		}

		private static IEnumerable<RavenJObject> YieldMemTableContents(MemTable memTable, Reference<Slice> target)
		{
			using (var iterator = memTable.NewIterator())
			{
				iterator.Seek(target.Value);
				while (iterator.IsValid)
				{
					target.Value = iterator.Key;
					using (var stream = iterator.CreateValueStream())
					{
						yield return RavenJObject.Load(new BsonReader(stream));
					}
					iterator.Next();
				}
			}
		}

		public async Task AppendAsync(RavenJObject data)
		{
			if (disposed)
				throw new ObjectDisposedException("EventStream");
			var nextEtag = _options.Status.NextEtag();
			data["@etag"] = nextEtag.ToString();
			data["@date"] = DateTime.UtcNow.ToString("o");

			using (var stream = new BufferPoolMemoryStream(_options.BufferPool))
			{
				var bsonWriter = new BsonWriter(stream);
				data.WriteTo(bsonWriter);
				bsonWriter.Flush();
				stream.Position = 0;
				var mine = new PendingWrite(stream, nextEtag);

				_pending.Enqueue(mine);

				while (mine.Done() == false && _pending.Peek() != mine)
				{
					await _writeCompletedEvent.WaitAsync();
				}

				if (mine.Done())
					return;

				await AppendInternalAsync(mine);
			}
		}

		private async Task AppendInternalAsync(PendingWrite mine)
		{
			using (await _asyncLock.LockAsync())
			{
				var completed = new List<PendingWrite>();
				try
				{
					if (mine.Done())
						return;

					await MakeRoomForWrite(mine.Stream.Length, mine.Etag);

					var seq = _options.Status.NextSeqeunce();

					var room = _currentBufferSize - _memTable.MemTable.ApproximateMemoryUsage;

					PendingWrite write;
					while (_pending.TryDequeue(out write))
					{
						var memoryHandle = _memTable.MemTable.Write(write.Stream);

						await _memTable.Log.Write7BitEncodedIntAsync(memoryHandle.Size);

						using (var stream = _memTable.MemTable.Read(memoryHandle))
						{
							await _memTable.Log.CopyFromAsync(stream);
						}

						_memTable.MemTable.Add(seq, ItemType.Value, EtagToSlice(write.Etag), memoryHandle);

						room -= memoryHandle.Size;

						completed.Add(write);

						PendingWrite next;
						if (_pending.TryPeek(out next) == false)
							break;

						if (next.Stream.Length > room) // don't use more than the current mem table allows
							break;
					}

					foreach (var pendingWrite in completed)
					{
						pendingWrite.Result.SetResult(null);
					}
				}
				catch (Exception e)
				{
					foreach (var pendingWrite in completed)
					{
						pendingWrite.Result.SetException(e);
					}
					throw;
				}
				finally
				{
					_writeCompletedEvent.Pulse();
				}
			}
		}

		private static Slice EtagToSlice(Etag etag)
		{
			return "events/" + etag;
		}

		// REQUIRE: _asyncLock is held
		private async Task MakeRoomForWrite(long size, Etag newEtag)
		{
			if (_memTable.MemTable.ApproximateMemoryUsage + size <= _currentBufferSize)
			{
				return;
			}

			var currentMemTableAge = (_memTable.MemTable.CreatedAt - DateTime.UtcNow);
			if (currentMemTableAge.TotalSeconds < 3) // if it is too young, we will try doubling it
			{
				_currentBufferSize = Math.Min(_currentBufferSize * 2, _options.MaxInMemoryBuffer);
			}

			await FlushMemTableToFiskAsync(newEtag);
		}

		// REQUIRE: no other concurrent access to the event stream
		public async Task FlushMemTableToFiskAsync(Etag newEtag)
		{
			if (_flushingMemTable != null)
			{
				// we are currently writing this to disk, let us wait for that
				try
				{
					await _flushingMemTable;
				}
				finally
				{
					_flushingMemTable = null;
					_immutableMemTable = null;
				}
			}

			if (_memTable.MemTable.Count == 0)
			{
				return;
			}

			var imm = _memTable;

			_flushingMemTable = Task.Factory.StartNew(() => WriteMemTableToDisk(imm));

			lock (_metadataLocker)
			{
				_immutableMemTable = _memTable;
				_flushingMemTable = _flushingMemTable.ContinueWith(task =>
					{
						using (imm)
						{
							_immutableMemTable = null;
						}
					});
				long log;
				_memTable = new MemRange
					{
						MemTable = new MemTable(_currentBufferSize, _options.Comparator, _options.BufferPool),
						Start = EtagToSlice(newEtag),
						Log = new LogWriter(_options.CreateNewLogFile(out log), _options.BufferPool),
						LogNumber = log
					};
			}
		}

		public Task FlushingMemTable
		{
			get { return _flushingMemTable; }
		}


		private void WriteMemTableToDisk(MemRange range)
		{
			string fileName;
			Slice first = new Slice(), last = new Slice();
			using (var stream = _options.CreateNewTableFile(out fileName))
			using (var builder = new TableBuilder(_options, stream, _options.Storage.CreateTemp))
			using (var it = range.MemTable.NewIterator())
			{
				it.SeekToFirst();
				while (it.IsValid)
				{
					if (first.Array == null)
						first = it.Key;
					last = it.Key;
					using (var valueStream = it.CreateValueStream())
						builder.Add(it.Key, valueStream);
					it.Next();
				}
				builder.Finish();
				_options.Storage.Flush(stream);
			}
			lock (_metadataLocker)
			{
				_options.Status.Ranges = new List<CurrentStatus.SstRange>(_options.Status.Ranges)
					{
						new CurrentStatus.SstRange
							{
								End = last,
								Name = fileName,
								Start = first,
								Count = range.MemTable.Count
							}
					};
				_options.Status.LastCompletedLog = range.LogNumber;
				_options.FlushStatus();
			}
		}

		public async Task DisposeAsync()
		{
			if (disposed)
				return;
			disposed = true;
			var flushingMemTable = _flushingMemTable;
			if (flushingMemTable != null)
				await flushingMemTable;
				
			await FlushMemTableToFiskAsync(Etag.Empty);
			flushingMemTable = _flushingMemTable;
			if (flushingMemTable != null)
				await flushingMemTable;

			using(_memTable)
			using (_immutableMemTable)
			{
				foreach (var table in _tables.Values)
				{
					if(table.IsValueCreated)
						table.Value.Dispose();
				}
			}
		}

		public void Dispose()
		{
			DisposeAsync().Wait();
		}


		public class MemRange : IDisposable
		{
			public Slice Start { get; set; }
			public MemTable MemTable { get; set; }
			public LogWriter Log { get; set; }
			public long LogNumber { get; set; }

			public void Dispose()
			{
				using(Log)
				using (MemTable)
				{
					
				}
			}
		}
	}
}
