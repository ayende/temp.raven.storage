using System.Diagnostics;

namespace Raven.Storage.Impl.Compactions
{
	using Raven.Storage.Util;

	public class BackgroundCompactor : Compactor
	{
		private ManualCompactor manualCompactor;

		public BackgroundCompactor(StorageState state) : base(state)
		{
		}

		public ManualCompactor Manual
		{
			get { return manualCompactor ?? (manualCompactor = new ManualCompactor(state)); }
		}

		protected override bool IsManual
		{
			get { return false; }
		}

		public void MaybeScheduleCompaction(AsyncLock.LockScope locker)
		{
			Debug.Assert(locker != null && locker.Locked);

			if (state.BackgroundCompactionScheduled)
			{
				return; // already scheduled, nothing to do
			}
			if (state.ShuttingDown)
			{
				return;    // DB is being disposed; no more background compactions
			}
			if (manualCompactor != null && manualCompactor.InProgress)
			{
				return; // manual already in progress
			}
			if (state.ImmutableMemTable == null &&
				state.VersionSet.NeedsCompaction == false)
			{
				// No work to be done
				return;
			}

			Background.Work(ScheduleCompactionAsync());
		}

		protected override Compaction CompactionToProcess()
		{
			return state.VersionSet.PickCompaction();
		}
	}
}