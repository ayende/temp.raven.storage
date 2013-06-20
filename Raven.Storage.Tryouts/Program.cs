using System;
using System.Diagnostics;
using Raven.Aggregation.Tests;
using Raven.Storage.Benchmark.Generators;
using Raven.Storage.Data;

namespace Raven.Storage.Tryouts
{
	public class Program
	{
		public static void Main(string[] args)
		{
			var generator = new RandomGenerator();

			var sp = Stopwatch.StartNew();
			using (var storage = new Storage("test", new StorageOptions()))
			{
				storage.InitAsync().Wait();
				for (var i = 0; i < 100*1000; i += 1)
				{
					var batch = new WriteBatch();
					{
						var k = i;
						var key = string.Format("{0:0000000000000000}", k);
						batch.Put(key, generator.Generate(100));
					}

					storage.Writer.WriteAsync(batch, new WriteOptions
						{
							FlushToDisk = true
						}).Wait();
					if (i%1000 == 0)
					{
						Console.WriteLine("{0:#,#} {1}", i, sp.Elapsed);
					}
				}
			}

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
