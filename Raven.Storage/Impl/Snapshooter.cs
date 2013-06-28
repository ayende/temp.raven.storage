namespace Raven.Storage.Impl
{
	using System;
	using System.Collections.Generic;
	using System.Threading.Tasks;

	using Raven.Storage.Data;
	using Raven.Storage.Impl.Streams;

	public class Snapshooter
	{
		private readonly IStorageContext storageContext;

		private readonly List<Snapshot> snapshots;

		public IReadOnlyList<Snapshot> Snapshots
		{
			get
			{
				return snapshots.AsReadOnly();
			}
		} 

		public Snapshooter(IStorageContext storageContext)
		{
			this.storageContext = storageContext;
			snapshots = new List<Snapshot>();
		}

		public async Task<Snapshot> CreateNewSnapshotAsync(VersionSet versionSet, AsyncLock.LockScope locker)
		{
			await locker.LockAsync().ConfigureAwait(false);
			var snapshot = new Snapshot
				               {
					               Sequence = versionSet.LastSequence
				               };

			snapshots.Add(snapshot);

			return snapshot;
		}

		public async Task DeleteAsync(Snapshot snapshot, AsyncLock.LockScope locker)
		{
			await locker.LockAsync().ConfigureAwait(false);
			if (snapshots.Contains(snapshot))
				snapshots.Remove(snapshot);
		}

		public async Task WriteSnapshotAsync(LogWriter logWriter, VersionSet versionSet, AsyncLock.LockScope locker)
		{
			await locker.LockAsync().ConfigureAwait(false);

			var edit = new VersionEdit();
			AddMetadata(edit, storageContext.Options);
			AddCompactionPointers(edit, versionSet);
			AddFiles(edit, versionSet);

			await edit.EncodeToAsync(logWriter).ConfigureAwait(false);
		}

		private static void AddFiles(VersionEdit edit, VersionSet versionSet)
		{
			for (int level = 0; level < Config.NumberOfLevels; level++)
			{
				var files = versionSet.Current.Files[level];
				foreach (var file in files)
				{
					edit.AddFile(level, file);
				}
			}
		}

		private static void AddCompactionPointers(VersionEdit edit, VersionSet versionSet)
		{
			for (int level = 0; level < Config.NumberOfLevels; level++)
			{
				var compactionPointer = versionSet.CompactionPointers[level];
				if (!compactionPointer.IsEmpty())
				{
					edit.SetCompactionPointer(level, new InternalKey(compactionPointer));
				}
			}
		}

		private static void AddMetadata(VersionEdit edit, StorageOptions options)
		{
			edit.SetComparatorName(options.Comparator.Name);
		}
	}
}