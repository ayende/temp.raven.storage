namespace Raven.Storage.Impl
{
	using System.Collections.Concurrent;
	using System.Collections.ObjectModel;
	using System.Threading.Tasks;

	using Raven.Storage.Data;
	using Raven.Storage.Impl.Streams;

	public class Snapshooter
	{
		private readonly StorageState storageContext;

		private readonly ConcurrentDictionary<Snapshot, object> snapshots;

		public ReadOnlyCollection<Snapshot> Snapshots
		{
			get
			{
				return (ReadOnlyCollection<Snapshot>) snapshots.Keys;
			}
		}

		public Snapshooter(StorageState storageContext)
		{
			this.storageContext = storageContext;
			snapshots = new ConcurrentDictionary<Snapshot, object>();
		}

		public Snapshot CreateNewSnapshot(VersionSet versionSet)
		{
			var snapshot = new Snapshot
				               {
					               Sequence = versionSet.LastSequence
				               };

			snapshots.TryAdd(snapshot, null);

			return snapshot;
		}

		public void Delete(Snapshot snapshot)
		{
			object _;
			snapshots.TryRemove(snapshot, out _);
		}

		public void WriteSnapshot(LogWriter logWriter, VersionSet versionSet)
		{
			var edit = new VersionEdit();
			AddMetadata(edit, storageContext.Options);
			AddCompactionPointers(edit, versionSet);
			AddFiles(edit, versionSet);

			edit.EncodeTo(logWriter);
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