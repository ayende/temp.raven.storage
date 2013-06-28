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
			try
			{
				for (int i = 0; i < 100; i++)
				{
					Benchmark.Program.Main(new[] { "--benchmarks=fill100k" });
				}
			}
			catch (Exception)
			{
				Console.ReadLine();
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
