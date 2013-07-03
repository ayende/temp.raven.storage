using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Raven.Storage.Impl;

namespace Raven.Storage.Util
{
	public class LruCache<TKey, TValue> : IDisposable
	{
		private readonly int _capacity;
		private readonly Stopwatch _stopwatch = Stopwatch.StartNew();

		private class Node
		{
			public TValue Value;
			public volatile Reference<long> Ticks;
		}

		private readonly ConcurrentDictionary<TKey, Node> _nodes;

		public LruCache(int capacity, IEqualityComparer<TKey> comparer = null)
		{
			_nodes = new ConcurrentDictionary<TKey, Node>(comparer ?? EqualityComparer<TKey>.Default);
			Debug.Assert(capacity > 10);
			_capacity = capacity;
		}

		public void Set(TKey key, TValue value)
		{
			var node = new Node
			{
				Value = value,
				Ticks = new Reference<long> { Value = _stopwatch.ElapsedTicks }
			};

			_nodes.AddOrUpdate(key, node, (_, __) => node);
			if (_nodes.Count <= _capacity)
				return;
			TrimCache();
		}

		private void TrimCache()
		{
			foreach (var source in _nodes.ToArray().OrderBy(x => x.Value.Ticks.Value).Take(_nodes.Count / 10))
			{
				Node removed;
				if (!_nodes.TryRemove(source.Key, out removed)) 
					continue;
				CleanupNode(removed);
			}
		}

		public bool TryGet(TKey key, out TValue value)
		{
			Node node;
			if (_nodes.TryGetValue(key, out node))
			{
				node.Ticks = new Reference<long> {Value = _stopwatch.ElapsedTicks};
				value = node.Value;
				return true;
			}
			value = default(TValue);
			return false;
		}

		public void Dispose()
		{
			foreach (var node in _nodes)
			{
				Node value = node.Value;
				CleanupNode(value);
			}
			_nodes.Clear();
		}

		private static void CleanupNode(Node value)
		{
			var disposable = value.Value as IDisposable;
			if (disposable == null) 
				return;
			try
			{
				disposable.Dispose();
			}
			catch (Exception)
			{
				// we don't allow the exception to propogate, may be on a different thread
				// and the caller almost certainly don't know how to handle this
			}
		}

		public void Remove(TKey key)
		{
			Node node;
			if(_nodes.TryRemove(key, out node))
				CleanupNode(node);	
		}
	}
}