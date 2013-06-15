using System;
using System.Threading.Tasks;

namespace Raven.Storage.Util
{
	public static class Background
	{
		public static void Work(Task t)
		{
			// this is here to avoid the 4014 compiler warning when we
			// explicitly want to start async background work
		}

		public static void Work(Action t)
		{
			Task.Run(t);
		} 
	}
}