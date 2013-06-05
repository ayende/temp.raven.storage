namespace Raven.Storage.Impl
{
	using System.IO;

	using Raven.Abstractions.Extensions;
	using Raven.Storage.Data;

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

			var reference = new Reference<Slice> { Value = key };

			Stream stream;
			GetStats stats;

			if (mem.TryGet(reference.Value, Format.MaxSequenceNumber, out stream))
			{
				return stream;
			}

			if (imm != null && imm.TryGet(reference.Value, Format.MaxSequenceNumber, out stream))
			{
				return stream;
			}

			if (currentVersion.TryGet(reference.Value, options, out stream, out stats))
			{
				if (currentVersion.UpdateStats(stats))
				{
					//this.state.MaybeScheduleCompaction();
				}

				return stream;
			}

			return null;
		}
	}
}