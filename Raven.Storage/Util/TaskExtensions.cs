using System.Threading.Tasks;

namespace Raven.Storage.Util
{
	public static class TaskExtensions
	{
		public static bool IsRunning(this Task task)
		{
			return (task.IsCompleted || task.IsCanceled || task.IsFaulted) == false;
		}

		public static void AssertNotFaulted(this Task task)
		{
			if (task.IsFaulted)
				task.Wait(); // will throw
		}
	}
}