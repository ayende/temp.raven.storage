using System;
using System.Diagnostics;
using System.Threading;
using Raven.Storage.Reading;

namespace Raven.Storage.Memtable
{
	using System.Collections.Generic;
	using System.Runtime.CompilerServices;

	/// <summary>
	///  Thread safety
	///  -------------
	/// 
	///  Writes require external synchronization, most likely a mutex.
	///  Reads require a guarantee that the SkipList will not be destroyed
	///  while the read is in progress.  Apart from that, reads progress
	///  without any internal locking or synchronization.
	/// </summary>
	public class SkipList<TKey, TVal>
	{
		private readonly IComparer<TKey> _comparer;
		public const int SkipListMaxHeight = 12;

		private readonly Node head = new Node(default(TKey), default(TVal), SkipListMaxHeight);

		// Modified only by Insert, allowed to be read racily by readers
		private int _maxHeight = 1;

		// Only used by Insert, which require externa syncronizastion
		Random _rnd = new Random();

		public int MaxHeight
		{
			get { return Volatile.Read(ref _maxHeight); }
		}

		public int Count { get; private set; }

		public SkipList(IComparer<TKey> comparer)
		{
			_comparer = comparer;
		}

		public bool Contains(TKey key)
		{
			var x = FindGreaterOrEqual(key, null);
			return (x != null && Equal(key, x.Key));
		}

		public void Insert(TKey key, TVal val)
		{
			var prev = new Node[SkipListMaxHeight];
			Node x = FindGreaterOrEqual(key, prev);

			// Our data structure does not allow duplicate insertion
			if (x != null && Equal(key, x.Key))
				throw new InvalidOperationException("Key already exists");

			int height = RandomHeight();
			if (height > MaxHeight)
			{
				for (int i = MaxHeight; i < height; i++)
				{
					prev[i] = head;
				}

				// It is ok to mutate max_height_ without any synchronization
				// with concurrent readers.  A concurrent reader that observes
				// the new value of max_height_ will see either the old value of
				// new level pointers from head_ (NULL), or a new value set in
				// the loop below.  In the former case the reader will
				// immediately drop to the next level since NULL sorts after all
				// keys.  In the latter case the reader will use the new node.
				_maxHeight = height;
			}

			x = new Node(key, val, height);
			for (int i = 0; i < height; i++)
			{
				// NoBarrier_SetNext() suffices since we will add a barrier when
				// we publish a pointer to "x" in prev[i].
				x.SetNextWithNoBarrier(i, prev[i].GetNextWithNoBarrier(i));
				prev[i].SetNext(i, x);
			}

			Count ++;
		}

		private Node FindGreaterOrEqual(TKey key, Node[] prev)
		{
			Node x = head;
			int level = MaxHeight - 1;
			while (true)
			{
				var next = x.Next(level);
				if (KeyIsAfterNode(key, next))
				{
					// Keep searching in this list
					x = next;
				}
				else
				{
					if (prev != null)
						prev[level] = x;
					if (level == 0)
					{
						return next;
					}
					// Switch to next list
					level--;
				}
			}
		}

		private Node FindLessThan(TKey key)
		{
			Node x = head;
			int level = MaxHeight - 1;
			while (true)
			{
				Debug.Assert(x == head || _comparer.Compare(x.Key, key) < 0);
				Node next = x.Next(level);
				if (next == null || _comparer.Compare(next.Key, key) >= 0)
				{
					if (level == 0)
					{
						return x;
					}
					// Switch to next list
					level--;
				}
				else
				{
					x = next;
				}
			}
		}

		private Node FindLast()
		{
			Node x = head;
			int level = MaxHeight - 1;
			while (true)
			{
				Node next = x.Next(level);
				if (next == null)
				{
					if (level == 0)
					{
						return x;
					}
					// Switch to next list
					level--;
				}
				else
				{
					x = next;
				}
			}
		}

		private bool Equal(TKey a, TKey b)
		{
			return _comparer.Compare(a, b) == 0;
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		private bool KeyIsAfterNode(TKey key, Node n)
		{
			// NULL n is considered infinite
			return (n != null) && (_comparer.Compare(n.Key, key) < 0);
		}

		private int RandomHeight()
		{
			// Increase height with probability 1 in kBranching
			const int branching = 4;
			int height = 1;
			while (height < MaxHeight && ((_rnd.Next() % branching) == 0))
			{
				height++;
			}
			Debug.Assert(height > 0);
			Debug.Assert(height <= SkipListMaxHeight);
			return height;
		}


		public class Node
		{
			private readonly TVal _val;
			public TKey Key { get; private set; }
			private readonly Node[] next;

			public Node(TKey key, TVal val, int height)
			{
				_val = val;
				Key = key;
				next = new Node[height];
			}

			public TVal Val
			{
				get { return _val; }
			}

			public Node Next(int i)
			{
				return Volatile.Read(ref next[i]);
			}

			public void SetNext(int i, Node val)
			{
				Volatile.Write(ref next[i], val);
			}

			public Node GetNextWithNoBarrier(int i)
			{
				return next[i];
			}

			public void SetNextWithNoBarrier(int i, Node val)
			{
				next[i] = val;
			}
		}

		public class Iterator
		{
			public SkipList<TKey, TVal> parent;
			private Node node;

			public Iterator(SkipList<TKey, TVal> parent)
			{
				this.parent = parent;
			}

			public bool IsValid
			{
				get
				{
					return node != null;
				}
			}

			public TKey Key
			{
				get
				{
					return node.Key;
				}
			}

			public TVal Val
			{
				get { return node.Val; }
			}

			public void Next()
			{
				node = node.Next(0);
			}

			public void Prev()
			{
				// Instead of using explicit "prev" links, we just search for the
				// last node that falls before key.
				node = parent.FindLessThan(node.Key);
				if (node == parent.head)
				{
					node = null;
				}
			}

			public void Seek(TKey key)
			{
				node = parent.FindGreaterOrEqual(key, null);
			}

			public void SeekToFirst()
			{
				node = parent.head.Next(0);
			}

			public void SeekToLast()
			{
				node = parent.FindLast();
				if (node == parent.head)
				{
					node = null;
				}
			}
		}

		public Iterator NewIterator()
		{
			return new Iterator(this);
		}
	}

	public class SkipList<TKey> : SkipList<TKey, TKey>
	{
		public SkipList(IComparer<TKey> comparer)
			: base(comparer)
		{
		}

		public void Insert(TKey key)
		{
			Insert(key, key);
		}
	}
}