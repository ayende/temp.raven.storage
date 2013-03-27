using System;
using System.Collections.Generic;
using System.Globalization;
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
			var skiplist = new SkipList<string>(string.Compare);
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
			var skiplist = new SkipList<string>(string.Compare);
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

					var indexOf = orderedKeys.FindIndex(s => String.Compare(item, s, StringComparison.Ordinal)  <= 0);
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
	}
}