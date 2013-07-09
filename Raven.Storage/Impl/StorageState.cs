using System.Threading;
using Raven.Temp.Logging;

namespace Raven.Storage.Impl
{
	using System.Collections.Generic;
	using System.IO;
	using System.Linq;

	using Raven.Storage.Building;
	using Raven.Storage.Comparing;
	using Raven.Storage.Data;
	using Raven.Storage.Exceptions;
	using Raven.Storage.Impl.Caching;
	using Raven.Storage.Impl.Compactions;
	using Raven.Storage.Impl.Streams;
	using Raven.Storage.Memtable;
	using System;
	using System.Diagnostics;
	using System.Threading.Tasks;

	using Raven.Storage.Reading;

	public class StorageState : IDisposable
	{
		private static readonly ILog Log = LogManager.GetCurrentClassLogger();

		public MemTable MemTable;
		public volatile MemTable ImmutableMemTable;
		public volatile bool BackgroundCompactionScheduled;
		public volatile Task BackgroundTask = Task.FromResult<object>(null);
		private readonly CancellationTokenSource _cancellationTokenSource;

		public CancellationToken CancellationToken;

		public LogWriter LogWriter { get; private set; }

		public LogWriter DescriptorLogWriter { get; private set; }

		public AsyncLock Lock { get; private set; }
		public VersionSet VersionSet { get; private set; }
		public StorageOptions Options { get; private set; }
		public FileSystem FileSystem { get; set; }
		public string DatabaseName { get; private set; }
		public ulong LogFileNumber { get; private set; }

		public CompactionStats[] CompactionStats = new CompactionStats[Config.NumberOfLevels];
		private PerfCounters _perfCounters;

		public TableCache TableCache { get; private set; }

		public BackgroundCompactor Compactor { get; private set; }

		public InternalKeyComparator InternalKeyComparator { get; private set; }

		public Snapshooter Snapshooter { get; private set; }

		public StorageState(string name, StorageOptions options)
		{
			_perfCounters = new PerfCounters(name);
			_cancellationTokenSource = new CancellationTokenSource();
			CancellationToken = _cancellationTokenSource.Token;
			Options = options;
			InternalKeyComparator = new InternalKeyComparator(options.Comparator);
			DatabaseName = name;
			Lock = new AsyncLock();
			FileSystem = new FileSystem(DatabaseName);
			MemTable = new MemTable(this);
			TableCache = new TableCache(this);
			VersionSet = new VersionSet(this);
			Compactor = new BackgroundCompactor(this);
			Snapshooter = new Snapshooter(this);
		}

		public PerfCounters PerfCounters
		{
			get { return _perfCounters; }
		}

		public void CreateNewLog()
		{
			var newFileNumber = VersionSet.NewFileNumber();
			try
			{
				var name = FileSystem.GetLogFileName(newFileNumber);
				var file = FileSystem.NewWritable(name);
				LogWriter = new LogWriter(FileSystem, file, Options.BufferPool);
				LogFileNumber = newFileNumber;
			}
			catch (Exception)
			{
				// Avoid chewing through file number space in a tight loop.
				VersionSet.ReuseFileNumber(newFileNumber);
				throw;
			}
		}

		public async Task LogAndApplyAsync(VersionEdit edit, AsyncLock.LockScope locker)
		{
			string newManifestFile = null;

			try
			{
				Version version;

				using (await locker.LockAsync())
				{
					if (!edit.LogNumber.HasValue) edit.SetLogNumber(VersionSet.LogNumber);
					else if (edit.LogNumber < VersionSet.LogNumber || edit.LogNumber >= VersionSet.NextFileNumber)
						throw new InvalidOperationException("LogNumber");

					if (!edit.PrevLogNumber.HasValue) edit.SetPrevLogNumber(VersionSet.PrevLogNumber);

					edit.SetNextFile(VersionSet.NextFileNumber);
					edit.SetLastSequence(VersionSet.LastSequence);

					version = new Version(this, VersionSet);

					var builder = new Builder(this, VersionSet, VersionSet.Current);
					builder.Apply(edit);
					builder.SaveTo(version);

					Version.Finalize(version);

					// Initialize new descriptor log file if necessary by creating
					// a temporary file that contains a snapshot of the current version.

					if (DescriptorLogWriter == null)
					{
						// No reason to unlock *mu here since we only hit this path in the
						// first call to LogAndApply (when opening the database).

						newManifestFile = FileSystem.DescriptorFileName(VersionSet.ManifestFileNumber);
						edit.SetNextFile(VersionSet.NextFileNumber);
						var descriptorFile = FileSystem.NewWritable(newManifestFile);

						DescriptorLogWriter = new LogWriter(FileSystem, descriptorFile, Options.BufferPool);

						Snapshooter.WriteSnapshot(DescriptorLogWriter, VersionSet);
					}
				}

				// Write new record to MANIFEST log

				edit.EncodeTo(DescriptorLogWriter);

				// If we just created a new descriptor file, install it by writing a
				// new CURRENT file that points to it.
				if (!string.IsNullOrEmpty(newManifestFile))
				{
					SetCurrentFile(VersionSet.ManifestFileNumber);
					// No need to double-check MANIFEST in case of error since it
					// will be discarded below.
				}

				using (await locker.LockAsync())
				{
					// Install the new version
					VersionSet.AppendVersion(version);
					VersionSet.SetLogNumber(edit.LogNumber.Value);
					VersionSet.SetPrevLogNumber(edit.PrevLogNumber.Value);
				}
			}
			catch (Exception)
			{
				if (!string.IsNullOrEmpty(newManifestFile))
				{
					if (DescriptorLogWriter != null)
					{
						DescriptorLogWriter.Dispose();
						DescriptorLogWriter = null;
					}

					FileSystem.DeleteFile(newManifestFile);
				}

				throw;
			}
		}

		private void SetCurrentFile(ulong descriptorNumber)
		{
			var manifest = FileSystem.DescriptorFileName(descriptorNumber);
			var tempFileName = FileSystem.GetTempFileName(descriptorNumber);

			using (var writer = new StreamWriter(FileSystem.NewWritable(tempFileName)))
			{
				writer.Write(manifest);
				writer.Flush();
			}

			FileSystem.RenameFile(tempFileName, FileSystem.GetCurrentFileName());
		}

		public VersionEdit Recover()
		{
			FileSystem.EnsureDatabaseDirectoryExists();
			FileSystem.Lock();

			if (FileSystem.Exists(FileSystem.GetCurrentFileName()) == false)
			{
				if (Options.CreateIfMissing)
				{
					CreateNewDatabase();
				}
				else
				{
					throw new InvalidDataException(DatabaseName + " does not exist. Storage option CreateIfMissing is set to false.");
				}
				Log.Info("Creating db {0} from scratch", DatabaseName);
			}
			else
			{
				if (Options.ErrorIfExists)
				{
					throw new InvalidDataException(DatabaseName + " exists, while the ErrorIfExists option is set to true.");
				}
				Log.Info("Loading db {0} from existing source", DatabaseName);
			}

			VersionSet.Recover();
			var minLog = VersionSet.LogNumber;
			var prevLog = VersionSet.PrevLogNumber;

			var databaseFiles = FileSystem.GetFiles();

			var expected = VersionSet.GetLiveFiles();

			var logNumbers = new List<ulong>();

			foreach (var databaseFile in databaseFiles)
			{
				ulong number;
				FileType fileType;
				if (FileSystem.TryParseDatabaseFile(databaseFile, out number, out fileType))
				{
					expected.Remove(number);

					if (fileType == FileType.LogFile && ((number >= minLog) || (number == prevLog)))
					{
						logNumbers.Add(number);
					}
				}
			}

			if (expected.Count > 0)
			{
				throw new CorruptedDataException(string.Format("Cannot recover because there are {0} missing files", expected.Count));
			}

			logNumbers.Sort();

			ulong maxSequence = 0;
			var edit = new VersionEdit();

			foreach (var logNumber in logNumbers)
			{
				maxSequence = RecoverLogFile(logNumber, edit, maxSequence);
				VersionSet.MarkFileNumberUsed(logNumber);
			}

			if (VersionSet.LastSequence < maxSequence)
			{
				VersionSet.LastSequence = maxSequence;
			}

			return edit;
		}

		private ulong RecoverLogFile(ulong logNumber, VersionEdit edit, ulong maxSequence)
		{
			var logFileName = FileSystem.GetLogFileName(logNumber);

			Log.Info("Starting to recover from log: {0}", logFileName);

			MemTable mem = null;
			using (var logFile = FileSystem.OpenForReading(logFileName))
			{
				foreach (var item in WriteBatch.ReadFromLog(logFile, Options.BufferPool))
				{
					var lastSequence = item.WriteSequence + (ulong)item.WriteBatch.OperationCount - 1;

					if (lastSequence > maxSequence)
					{
						maxSequence = lastSequence;
					}

					if (mem == null)
					{
						mem = new MemTable(this);
					}

					item.WriteBatch.Prepare(mem);
					item.WriteBatch.Apply(mem, new Reference<ulong> { Value = item.WriteSequence });

					if (mem.ApproximateMemoryUsage > Options.WriteBatchSize)
					{
						Compactor.WriteLevel0Table(mem, null, edit);
						mem = null;
					}
				}
			}

			if (mem != null)
			{
				Compactor.WriteLevel0Table(mem, null, edit);
			}

			return maxSequence;
		}

		/// <summary>
		/// Build a Table file from the contents of *iter.  The generated file
		/// will be named according to meta->number.  On success, the rest of
		/// *meta will be filled with metadata about the generated table.
		/// If no data is present in *iter, meta->file_size will be set to
		/// zero, and no Table file will be produced.
		/// </summary>
		/// <param name="memTable"></param>
		/// <param name="fileNumber"></param>
		/// <returns></returns>
		public FileMetadata BuildTable(MemTable memTable, ulong fileNumber)
		{
			CancellationToken.ThrowIfCancellationRequested();

			TableBuilder builder = null;
			var meta = new FileMetadata
						   {
							   FileNumber = fileNumber
						   };

			var tableFileName = FileSystem.GetTableFileName(fileNumber);
			try
			{
				var iterator = memTable.NewIterator();
				iterator.SeekToFirst();

				if (Log.IsDebugEnabled)
					Log.Debug("Writing table with {0:#,#;;00} items to {1}", memTable.Count, tableFileName);

				if (iterator.IsValid)
				{
					var tableFile = FileSystem.NewWritable(tableFileName);
					builder = new TableBuilder(this, tableFile, new TemporaryFiles(FileSystem, fileNumber));

					meta.SmallestKey = new InternalKey(iterator.Key);
					while (iterator.IsValid)
					{
						CancellationToken.ThrowIfCancellationRequested();
						var key = iterator.Key;

						meta.LargestKey = new InternalKey(key);

						if (Log.IsDebugEnabled)
							Log.Debug("Writing item with key {0}", meta.LargestKey.DebugVal);

						using (var stream = iterator.CreateValueStream())
							builder.Add(key, stream);

						iterator.Next();
					}

					builder.Finish();

					meta.FileSize = builder.FileSize;
					Debug.Assert(meta.FileSize > 0);
				}
			}
			finally
			{
				if (builder != null)
					builder.Dispose();

				if (meta.FileSize == 0)
				{
					FileSystem.DeleteFile(tableFileName);
				}
			}

			return meta;
		}

		public void CreateNewDatabase()
		{
			var newDb = new VersionEdit();

			newDb.SetComparatorName(Options.Comparator.Name);
			newDb.SetLogNumber(0);
			newDb.SetNextFile(2);
			newDb.SetLastSequence(0);

			var manifest = FileSystem.DescriptorFileName(1);

			try
			{
				using (var file = FileSystem.NewWritable(manifest))
				{
					using (var logWriter = new LogWriter(FileSystem, file, Options.BufferPool))
					{
						newDb.EncodeTo(logWriter);
					}
				}

				SetCurrentFile(1);
			}
			catch (Exception)
			{
				FileSystem.DeleteFile(manifest);

				throw;
			}
		}

		public void Dispose()
		{
			_cancellationTokenSource.Cancel();

			if (BackgroundTask != null)
			{
				try
				{
					BackgroundTask.Wait();
				}
				catch (Exception e)
				{
					Log.ErrorException("Failure in background task", e);
				}
			}

			if (LogWriter != null)
				LogWriter.Dispose();
			if (DescriptorLogWriter != null)
				DescriptorLogWriter.Dispose();
			if (FileSystem != null)
				FileSystem.Dispose();
			if (MemTable != null)
				MemTable.Dispose();
			if (ImmutableMemTable != null)
				ImmutableMemTable.Dispose();
			if (TableCache != null)
				TableCache.Dispose();

			if (_perfCounters != null)
				_perfCounters.Dispose();
		}

		public Tuple<IIterator, ulong> NewInternalIterator(ReadOptions options)
		{
			var mem = MemTable;
			var imm = ImmutableMemTable;
			var currentVersion = VersionSet.Current;

			var snapshot = options.Snapshot != null ? options.Snapshot.Sequence : VersionSet.LastSequence;

			var iterators = new List<IIterator>
				                {
					                mem.NewIterator()
				                };

			if (imm != null)
				iterators.Add(imm.NewIterator());

			// Merge all level zero files together since they may overlap
			iterators.AddRange(currentVersion.Files[0].Select(file => TableCache.NewIterator(options, file.FileNumber, file.FileSize)));

			// For levels > 0, we can use a concatenating iterator that sequentially
			// walks through the non-overlapping files in the level, opening them
			// lazily.
			for (var level = 1; level < Config.NumberOfLevels; level++)
			{
				if (currentVersion.Files[level].Count > 0)
					iterators.Add(new TwoLevelIterator(new LevelFileNumIterator(InternalKeyComparator, currentVersion.Files[level]), VersionSet.GetFileIterator, options));
			}

			var internalIterator = new MergingIterator(InternalKeyComparator, iterators);

			return new Tuple<IIterator, ulong>(internalIterator, snapshot);
		}

		internal async Task MakeRoomForWriteAsync(bool force, AsyncLock.LockScope lockScope)
		{
			bool allowDelay = force == false;
			while (true)
			{
				using (await lockScope.LockAsync())
				{
					if (BackgroundTask.IsCanceled || BackgroundTask.IsFaulted)
					{
						await BackgroundTask.ConfigureAwait(false); // throws
					}
					else if (allowDelay && VersionSet.GetNumberOfFilesAtLevel(0) >= Config.SlowdownWritesTrigger)
					{
						// We are getting close to hitting a hard limit on the number of
						// L0 files.  Rather than delaying a single write by several
						// seconds when we hit the hard limit, start delaying each
						// individual write by 1ms to reduce latency variance.  Also,
						// this delay hands over some CPU to the compaction thread in
						// case it is sharing the same core as the writer.
						lockScope.Exit();
						{
							await Task.Delay(TimeSpan.FromMilliseconds(1)).ConfigureAwait(false);
						}
						await lockScope.LockAsync().ConfigureAwait(false);
						allowDelay = false; // Do not delay a single write more than once
					}
					else if (force == false && MemTable.ApproximateMemoryUsage <= Options.WriteBatchSize)
					{
						// There is room in current memtable
						break;
					}
					else if (ImmutableMemTable != null)
					{
						Compactor.MaybeScheduleCompaction(lockScope);
						lockScope.Exit();

						// We have filled up the current memtable, but the previous
						// one is still being compacted, so we wait.
						await BackgroundTask.ConfigureAwait(false);
					}
					else if (VersionSet.GetNumberOfFilesAtLevel(0) >= Config.StopWritesTrigger)
					{
						Compactor.MaybeScheduleCompaction(lockScope);
						lockScope.Exit();

						// There are too many level-0 files.
						await BackgroundTask.ConfigureAwait(false);
					}
					else
					{
						// Attempt to switch to a new memtable and trigger compaction of old
						Debug.Assert(VersionSet.PrevLogNumber == 0);

						LogWriter.Dispose();

						CreateNewLog();
						ImmutableMemTable = MemTable;
						MemTable = new MemTable(this);
						force = false;
						Compactor.MaybeScheduleCompaction(lockScope);
					}
				}
			}
		}

		public async Task<StorageStatistics> GetStorageStatisticsAsync()
		{
			using (await Lock.LockAsync().ConfigureAwait(false))
			{
				var files = new List<FileMetadata>[Config.NumberOfLevels];
				for (var level = 0; level < Config.NumberOfLevels; level++)
				{
					files[level] = VersionSet.Current.Files[level]
						.Select(x => new FileMetadata(x))
						.ToList();
				}

				return new StorageStatistics(files);
			}
		}
	}
}
