using System;
using Raven.Aggregation.Tests;

namespace Raven.Storage.Tryouts
{
	public class Program
	{
		public static void Main(string[] args)
		{
			new ComplexAggregator().UsingMultiMap().Wait();

            //var storage = new Storage(@"D:\Work\Raven.Storage\Raven.Storage.Tryouts\bin\Debug\test", new StorageOptions());
            //storage.InitAsync().Wait();
            //var it = storage.StorageState.TableCache.NewIterator(new ReadOptions(), 5, 7684);
            //it.Seek(new InternalKey("raven/Test/1", 78, ItemType.Value).TheInternalKey);
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
