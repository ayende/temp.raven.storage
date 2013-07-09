using System;
using System.Threading.Tasks;

namespace Raven.Storage.Impl.Compactions
{
	using Raven.Storage.Data;

	/// <summary>
	/// Information for a manual compaction
	/// </summary>
	public class ManualCompactor : Compactor
	{
		private Compaction compaction;

		public ManualCompactor(StorageState state)
			: base(state)
		{
		}

		public int Level { get; private set; }

		public bool Done { get; set; }

		/// <summary>
		/// NULL means beginning of key range
		/// </summary>
		public InternalKey Begin { get; private set; }

		/// <summary>
		/// NULL means end of key range
		/// </summary>
		public InternalKey End { get; private set; }

		public bool InProgress { get; set; }

		protected override Compaction CompactionToProcess()
		{
			compaction = state.VersionSet.CompactRange(Level, Begin, End);

			Done = (compaction == null);

			return compaction;
		}

		protected override bool IsManual
		{
			get { return true; }
		}

		public async Task CompactAsync(int level, Slice begin, Slice end)
		{
			if (InProgress)
				throw new InvalidOperationException("Manual compaction is already in progess.");

			try
			{
				InProgress = true;
				Done = false;

				Level = level;
				Begin = new InternalKey(begin, Format.MaxSequenceNumber, ItemType.ValueForSeek);
				End = new InternalKey(end, Format.MaxSequenceNumber, ItemType.ValueForSeek);

				Task task = null;

				while (task == null)
				{
					state.CancellationToken.ThrowIfCancellationRequested();

					if (state.BackgroundCompactionScheduled)
					{
						await Task.Delay(100).ConfigureAwait(false);
						continue;
					}

					using (AsyncLock.LockScope locker = await state.Lock.LockAsync().ConfigureAwait(false))
					{
						while (Done == false)
						{
							state.BackgroundCompactionScheduled = true;
							await RunCompactionAsync(locker).ConfigureAwait(false);

							var manualEnd = new InternalKey();

							if (compaction != null)
							{
								manualEnd = compaction.GetInput(0, compaction.GetNumberOfInputFiles(0) - 1).LargestKey;
							}

							if (Done == false)
							{
								// We only compacted part of the requested range. Update to the range that is left to be compacted.
								Begin = manualEnd;
							}
						}

						task = state.BackgroundTask;
					}
				}
			}
			finally
			{
				Done = true;
				InProgress = false;
			}
		}

		public async Task CompactRangeAsync(Slice begin, Slice end)
		{
			int maxLevelWithFiles = 1;
			using (var locker = await state.Lock.LockAsync().ConfigureAwait(false))
			{
				var @base = state.VersionSet.Current;
				for (var level = 1; level < Config.NumberOfLevels; level++)
				{
					if (@base.OverlapInLevel(level, begin, end))
					{
						maxLevelWithFiles = level;
					}
				}
			}

			for (var level = 0; level < maxLevelWithFiles; level++)
			{
				await CompactAsync(level, begin, end).ConfigureAwait(false);
			}
		}

		private Task EnsureTableFileCreated(AsyncLock.LockScope lockScope)
		{
			return state.MakeRoomForWriteAsync(force: true, lockScope: lockScope); // force to create an ImmutableMemtable
		}

		public async Task CompactMemTableAsync()
		{
			using (var locker = await state.Lock.LockAsync().ConfigureAwait(false))
			{
				await EnsureTableFileCreated(locker).ConfigureAwait(false);
			}

			while (state.BackgroundCompactionScheduled)
				await Task.Delay(50);
		}
	}
}