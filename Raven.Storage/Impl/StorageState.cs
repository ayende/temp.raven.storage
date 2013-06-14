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
	using System.Text;
	using System.Threading.Tasks;

	using Raven.Storage.Reading;

	public class StorageState : IDisposable, IStorageContext
	{
		public MemTable MemTable;
		public volatile MemTable ImmutableMemTable;
		public volatile bool BackgroundCompactionScheduled;
		public volatile Task BackgroundTask = Task.FromResult<object>(null);
		public volatile bool ShuttingDown;

		public LogWriter LogWriter { get; private set; }

		public LogWriter DescriptorLogWriter { get; private set; }

		public AsyncLock Lock { get; private set; }
		public VersionSet VersionSet { get; private set; }
		public StorageOptions Options { get; private set; }
		public FileSystem FileSystem { get; private set; }
		public string DatabaseName { get; private set; }
		public ulong LogFileNumber { get; private set; }

		public CompactionStats[] CompactionStats = new CompactionStats[Config.NumberOfLevels];

		public TableCache TableCache { get; private set; }

		public Compactor Compactor { get; private set; }

		public InternalKeyComparator InternalKeyComparator { get; private set; }

		public Snapshooter Snapshooter { get; private set; }

		public StorageState(string name, StorageOptions options)
		{
			Options = options;
			InternalKeyComparator = new InternalKeyComparator(options.Comparator);
			DatabaseName = name;
			Lock = new AsyncLock();
			FileSystem = new FileSystem(DatabaseName);
			MemTable = new MemTable(this);
			TableCache = new TableCache(this);
			VersionSet = new VersionSet(this);
			Compactor = new Compactor(this);
			Snapshooter = new Snapshooter(this);
		}

		public void CreateNewLog()
		{
			var newFileNumber = VersionSet.NewFileNumber();
			try
			{
				var file = FileSystem.NewWritable(newFileNumber, Constants.Files.Extensions.LogFile);
				LogWriter = new LogWriter(file, Options.BufferPool);
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
			await locker.LockAsync();

			string newManifestFile = null;

			try
			{
				if (!edit.LogNumber.HasValue)
					edit.SetLogNumber(VersionSet.LogNumber);
				else if (edit.LogNumber < VersionSet.LogNumber || edit.LogNumber >= VersionSet.NextFileNumber)
					throw new InvalidOperationException("LogNumber");

				if (!edit.PrevLogNumber.HasValue)
					edit.SetPrevLogNumber(VersionSet.PrevLogNumber);

				edit.SetNextFile(VersionSet.NextFileNumber);
				edit.SetLastSequence(VersionSet.LastSequence);

				var version = new Version(this, VersionSet);

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

					DescriptorLogWriter = new LogWriter(descriptorFile, Options.BufferPool);
					await Snapshooter.WriteSnapshotAsync(DescriptorLogWriter, VersionSet, locker);
				}

				// Unlock during expensive MANIFEST log write
				locker.Exit();

				// Write new record to MANIFEST log

				await edit.EncodeToAsync(DescriptorLogWriter);
				DescriptorLogWriter.Flush();

				//if (!s.ok()) {
				//	Log(options_->info_log, "MANIFEST write: %s\n", s.ToString().c_str());
				//	if (ManifestContains(record)) {
				//	  Log(options_->info_log,
				//		  "MANIFEST contains log record despite error; advancing to new "
				//		  "version to prevent mismatch between in-memory and logged state");
				//	  s = Status::OK();
				//	}
				//}

				// If we just created a new descriptor file, install it by writing a
				// new CURRENT file that points to it.
				if (!string.IsNullOrEmpty(newManifestFile))
				{
					await this.SetCurrentFileAsync(VersionSet.ManifestFileNumber);
					// No need to double-check MANIFEST in case of error since it
					// will be discarded below.
				}

				await locker.LockAsync();

				// Install the new version
				VersionSet.AppendVersion(version);
				VersionSet.SetLogNumber(edit.LogNumber.Value);
				VersionSet.SetPrevLogNumber(edit.PrevLogNumber.Value);
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

		private async Task SetCurrentFileAsync(ulong descriptorNumber)
		{
			var manifest = FileSystem.DescriptorFileName(descriptorNumber);
			var tempFileName = FileSystem.GetTempFileName(descriptorNumber);

			using (var writer = new StreamWriter(FileSystem.NewWritable(tempFileName)))
			{
				await writer.WriteAsync(manifest);
				await writer.FlushAsync();
			}

			FileSystem.RenameFile(tempFileName, FileSystem.GetCurrentFileName());
		}

		public async Task<VersionEdit> RecoverAsync()
		{
			FileSystem.EnsureDatabaseDirectoryExists();
			FileSystem.Lock();

			if (FileSystem.Exists(FileSystem.GetCurrentFileName()) == false)
			{
				if (Options.CreateIfMissing)
				{
					await CreateNewDatabaseAsync();
				}
				else
				{
					throw new InvalidDataException(DatabaseName + " does not exist. Storage option CreateIfMissing is set to false.");
				}
			}
			else
			{
				if (Options.ErrorIfExists)
				{
					throw new InvalidDataException(DatabaseName + " exists, while the ErrorIfExists option is set to true.");
				}
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
				RecoverLogFile(logNumber, ref edit, ref maxSequence);
				VersionSet.MarkFileNumberUsed(logNumber);
			}

			if (VersionSet.LastSequence < maxSequence)
			{
				VersionSet.LastSequence = maxSequence;
			}

			return edit;
		}

		private void RecoverLogFile(ulong logNumber, ref VersionEdit edit, ref ulong maxSequence)
		{
			var logFileName = FileSystem.GetLogFileName(logNumber);

			IList<LogReadResult> writeBatches;
			using (var logFile = FileSystem.OpenForReading(logFileName))
			{
				writeBatches = WriteBatch.ReadFromLog(logFile, Options.BufferPool);
			}

			MemTable mem = null;

			foreach (var item in writeBatches)
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
				item.WriteBatch.Apply(mem, item.WriteSequence);

				if (mem.ApproximateMemoryUsage > Options.WriteBatchSize)
				{
					Compactor.WriteLevel0Table(mem, null, edit);
					mem = null;
				}
			}

			if (mem != null)
			{
				Compactor.WriteLevel0Table(mem, null, edit);
			}
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

				if (iterator.IsValid)
				{
					var tableFile = FileSystem.NewWritable(tableFileName);
					builder = new TableBuilder(Options, tableFile, () => FileSystem.NewReadableWritable(FileSystem.GetTempFileName(fileNumber)));

					meta.SmallestKey = new InternalKey(iterator.Key);
					while (iterator.IsValid)
					{
						var key = iterator.Key;
						var stream = iterator.CreateValueStream();

						meta.LargestKey = new InternalKey(key);
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

		public async Task CreateNewDatabaseAsync()
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
					using (var logWriter = new LogWriter(file, Options.BufferPool))
					{
						await newDb.EncodeToAsync(logWriter);
					}
				}

				await this.SetCurrentFileAsync(1);
			}
			catch (Exception)
			{
				FileSystem.DeleteFile(manifest);

				throw;
			}
		}

		public void Dispose()
		{
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
		}

		public async Task<Tuple<IIterator, ulong>> NewInternalIteratorAsync(ReadOptions options, AsyncLock.LockScope locker)
		{
			await locker.LockAsync();

			var latestSnapshot = VersionSet.LastSequence;
			var iterators = new List<IIterator>
				                {
					                this.MemTable.NewIterator()
				                };

			if (ImmutableMemTable != null)
				iterators.Add(ImmutableMemTable.NewIterator());

			// Merge all level zero files together since they may overlap
			iterators.AddRange(this.VersionSet.Current.Files[0].Select(file => this.TableCache.NewIterator(options, file.FileNumber, file.FileSize)));

			// For levels > 0, we can use a concatenating iterator that sequentially
			// walks through the non-overlapping files in the level, opening them
			// lazily.
			for (var level = 1; level < Config.NumberOfLevels; level++)
			{
				if (VersionSet.Current.Files[level].Count > 0)
					iterators.Add(new LevelFileNumIterator(InternalKeyComparator, VersionSet.Current.Files[level]));
			}

			var internalIterator = new MergingIterator(InternalKeyComparator, iterators, iterators.Count);

			return new Tuple<IIterator, ulong>(internalIterator, latestSnapshot);
		}
	}
}
