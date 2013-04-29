using System.Collections.Concurrent;

namespace Raven.Storage.Impl
{
	internal static class ConcurrentQueueExtensions
	{
			public static T Peek<T>(this ConcurrentQueue<T> self)
				where T : class
			{
				T result;
				if (self.TryPeek(out result) == false)
					return null;
				return result;
			}
	}
}