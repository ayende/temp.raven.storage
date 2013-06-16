using System;
using System.Threading;
using NLog.Targets;
using Raven.Abstractions.Logging;
using Raven.Aggregation.Tests;
using Raven.Storage.Util;
using Target = Raven.Abstractions.Logging.Target;

namespace Raven.Storage.Tryouts
{
	public class Program
	{
		public static void Main(string[] args)
		{
            LogManager.RegisterTarget<MyConsoleTarget>();
			using (var x = new DoingAggregation())
			{
				var canAdd = x.CanAdd();
				while (canAdd.IsRunning())
				{
					Thread.Sleep(100);
				}
			}

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
