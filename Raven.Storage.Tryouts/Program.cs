using System.Threading;
using Raven.Aggregation.Tests;
using Raven.Storage.Util;

namespace Raven.Storage.Tryouts
{
	public class Program
	{
		public static void Main(string[] args)
		{
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
}
