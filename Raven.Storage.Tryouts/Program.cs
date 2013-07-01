using System;
using System.Collections.Generic;
using System.Diagnostics;
using Raven.Aggregation.Tests;
using Raven.Storage.Benchmark.Generators;
using Raven.Storage.Comparing;
using Raven.Storage.Data;
using Raven.Storage.Memtable;

namespace Raven.Storage.Tryouts
{
	public class Program
	{
		public static void Main(string[] args)
		{
			var sp = Stopwatch.StartNew();
			//var sl = new SkipList<InternalKey, object>(new InternalKeyComparator(new ByteWiseComparator()));
			var sl = new SortedDictionary<InternalKey, object>(new InternalKeyComparator(new ByteWiseComparator()));
			for (ulong i = 0; i < 1000*1000; i++)
			{
				//sl.Insert(new InternalKey(new Slice(Guid.NewGuid().ToByteArray()), i, ItemType.Value), null);
				sl.Add(new InternalKey(new Slice(Guid.NewGuid().ToByteArray()), i, ItemType.Value), null);
			}
			Console.WriteLine(sp.Elapsed);
		}
	}

	public class MyConsoleTarget : Raven.Abstractions.Logging.Target
	{
		public override void Write(Raven.Abstractions.Logging.LogEventInfo logEvent)
		{
			Console.WriteLine(logEvent.FormattedMessage);
		}
	}
}
