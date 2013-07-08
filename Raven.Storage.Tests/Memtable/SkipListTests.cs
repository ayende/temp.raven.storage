using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Raven.Storage.Memtable;
using Xunit;
using System.Linq;

namespace Raven.Storage.Tests.Memtable
{
	public class SkipListTests
	{
		[Fact]
		public void Empty()
		{
			var skiplist = new SkipList<string>(StringComparer.InvariantCultureIgnoreCase);
			Assert.False(skiplist.Contains("10"));

			var iterator = skiplist.NewIterator();
			Assert.False(iterator.IsValid);
			iterator.SeekToFirst();
			Assert.False(iterator.IsValid);
			iterator.Seek("100");
			Assert.False(iterator.IsValid);
			iterator.SeekToLast();
			Assert.False(iterator.IsValid);
		}

		[Fact]
		public void InsertAndLookup()
		{
			const int N = 2000;
			const int R = 5000;
			var rnd = new Random(1000);
			var skiplist = new SkipList<string>(StringComparer.InvariantCultureIgnoreCase);
			var keys = new List<string>();
			for (int i = 0; i < N; i++)
			{
				var key = rnd.Next() % R;
				var item = key.ToString(CultureInfo.InvariantCulture);
				if (keys.Contains(item) == false)
				{
					keys.Add(item);
					skiplist.Insert(item);
				}
			}

			for (int i = 0; i < R; i++)
			{
				var item = i.ToString(CultureInfo.InvariantCulture);
				if (skiplist.Contains(item))
				{
					Assert.True(keys.Contains(item));
				}
				else
				{
					Assert.False(keys.Contains(item));
				}
			}

			var orderedKeys = keys.OrderBy(x => x).ToList();


			// iterator tests
			{
				var iter = skiplist.NewIterator();
				Assert.False(iter.IsValid);

				iter.Seek("0");
				Assert.True(iter.IsValid);
				Assert.Equal(orderedKeys.First(), iter.Key);

				iter.SeekToFirst();
				Assert.True(iter.IsValid);
				Assert.Equal(orderedKeys.First(), iter.Key);

				iter.SeekToLast();
				Assert.True(iter.IsValid);
				Assert.Equal(orderedKeys.Last(), iter.Key);
			}
			// forward iteration
			{
				for (int i = 0; i < R; i++)
				{
					var iter = skiplist.NewIterator();
					var item = i.ToString(CultureInfo.InvariantCulture);
					iter.Seek(item);

					var indexOf = orderedKeys.FindIndex(s => String.Compare(item, s, StringComparison.Ordinal) <= 0);
					for (int j = 0; j < 3; j++)
					{
						if (indexOf == -1 || indexOf >= orderedKeys.Count)
						{
							Assert.False(iter.IsValid);
							break;
						}
						Assert.True(iter.IsValid);
						Assert.Equal(orderedKeys[indexOf], iter.Key);
						indexOf++;
						iter.Next();
					}
				}
			}

			// backward iteration
			{
				var iter = skiplist.NewIterator();
				iter.SeekToLast();
				for (int j = orderedKeys.Count - 1; j >= 0; j--)
				{
					Assert.True(iter.IsValid);
					Assert.Equal(orderedKeys[j], iter.Key);
					iter.Prev();
				}
				Assert.False(iter.IsValid);
			}
		}

		[Fact]
		public void InsertRemove()
		{
			const int N = 2000;
			const int R = 5000;
			var rnd = new Random(1000);
			var skiplist = new SkipList<string>(StringComparer.InvariantCultureIgnoreCase);
			var keys = new List<string>();
			for (int i = 0; i < N; i++)
			{
				var key = rnd.Next() % R;
				var item = key.ToString(CultureInfo.InvariantCulture);
				if (keys.Contains(item) == false)
				{
					keys.Add(item);
					skiplist.Insert(item);
				}
			}

			while (true)
			{
				if (keys.Count == 0)
					break;

				var keyToRemove = keys[rnd.Next(0, keys.Count - 1)];
				Assert.True(skiplist.Remove(keyToRemove));
				Assert.False(skiplist.Contains(keyToRemove));
				keys.Remove(keyToRemove);
			}

			Assert.Equal(0, skiplist.Count);
		}

		[Fact]
		public void ConcurrentTest()
		{
			var c = new ConcurrentDictionary<int, string>();
			var list = new SkipList<string>(StringComparer.InvariantCultureIgnoreCase);

			var reading = true;
			// we have one thread doing writes, and we make sure that all of the readers are always
			// able to read _at least_ the values that were present at the list at the time of read start

			const int readersCount = 10;
			var allReadersDone = new CountdownEvent(readersCount);
			var reader = (Action)(() =>
				{
					try
					{
						var rnd = new Random();
						while (Volatile.Read(ref reading))
						{
							var count = c.Count;
							if (count == 0)
								continue;
							var next = rnd.Next(0, count);
							Assert.True(list.Contains(c[next]));
							var iter = list.NewIterator();
							iter.Seek(c[next]);
							Assert.True(iter.IsValid);
							Assert.Equal(c[next], iter.Key);
							var shouldBeThere = new HashSet<string>(Enumerable.Range(next, count - next)
																			  .Select(i => c[i])
																			  .Where(x => String.CompareOrdinal(x, iter.Key) >= 0));
							while (iter.IsValid)
							{
								shouldBeThere.Remove(iter.Key);
								iter.Next();
							}
							Assert.Empty(shouldBeThere);
						}
					}
					finally
					{
						allReadersDone.Signal();
					}
				});

			var tasks = new List<Task>();
			for (int i = 0; i < readersCount; i++)
			{
				tasks.Add(Task.Factory.StartNew(reader));
			}

			for (int i = 0; i < 1000; i++)
			{
				var key = i.ToString(CultureInfo.InvariantCulture);
				list.Insert(key);
				c[i] = key;
			}
			Volatile.Write(ref reading, false);
			allReadersDone.Wait();

			Task.WaitAll(tasks.ToArray());
		}
	}
}