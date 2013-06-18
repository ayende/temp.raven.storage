using System;
using System.Threading;
using NLog.Targets;
using Raven.Abstractions.Logging;
using Raven.Aggregation.Tests;
using Raven.Storage.Data;
using Raven.Storage.Impl;
using Raven.Storage.Reading;
using Raven.Storage.Tests.Recovery;
using Raven.Storage.Util;
using Target = Raven.Abstractions.Logging.Target;

namespace Raven.Storage.Tryouts
{
	public class Program
	{
		public static void Main(string[] args)
		{
            LogManager.RegisterTarget<MyConsoleTarget>();

            new DoingAggregation().WillRememberAfterRestart().Wait();

            //var storage = new Storage(@"D:\Work\Raven.Storage\Raven.Storage.Tryouts\bin\Debug\test", new StorageOptions());
            //storage.InitAsync().Wait();
            //var it = storage.StorageState.TableCache.NewIterator(new ReadOptions(), 5, 7684);
            //it.Seek(new InternalKey("raven/Test/1", 78, ItemType.Value).TheInternalKey);
		}
	}

    public class MyConsoleTarget : Target
    {
        public override void Write(LogEventInfo logEvent)
        {
            Console.WriteLine(logEvent.FormattedMessage);
        }
    }
}
