namespace Raven.Storage.Impl
{
	using System.IO;
	using System.Threading.Tasks;

	using Raven.Abstractions.Extensions;
	using Raven.Storage.Data;
	using Raven.Storage.Reading;

	public class StorageReader
	{
		private readonly StorageState state;

		public StorageReader(StorageState state)
		{
			this.state = state;
		}

		public Stream Read(Slice key, ReadOptions options = null)
		{
			if (options == null)
			{
				options = new ReadOptions();
			}

			var mem = state.MemTable;
			var imm = state.ImmutableMemTable;
			var currentVersion = state.VersionSet.Current;

			var snapshot = options.Snapshot != null ? options.Snapshot.Sequence : this.state.VersionSet.LastSequence;

			var reference = new Reference<Slice> { Value = key };

			Stream stream;
			GetStats stats;

			if (mem.TryGet(reference.Value, snapshot, out stream))
			{
				return stream;
			}

			if (imm != null && imm.TryGet(reference.Value, snapshot, out stream))
			{
				return stream;
			}

			if (currentVersion.TryGet(reference.Value, snapshot, options, out stream, out stats))
			{
				if (currentVersion.UpdateStats(stats))
				{
					//this.state.MaybeScheduleCompactionAsync();
				}

				return stream;
			}

			return null;
		}

		public async Task<DbIterator> NewIteratorAsync(ReadOptions options)
		{
			using (var locker = await state.Lock.LockAsync())
			{
				var result = await state.NewInternalIteratorAsync(options, locker);
				var internalIterator = result.Item1;
				var latestSnapshot = result.Item2;

				return new DbIterator(state, internalIterator, options.Snapshot != null ? options.Snapshot.Sequence : latestSnapshot);
			}
		}
	}
}