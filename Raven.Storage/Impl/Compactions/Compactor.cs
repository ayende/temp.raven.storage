namespace Raven.Storage.Impl.Compactions
{
	using System;
	using System.Collections.Generic;
	using System.Diagnostics;
	using System.IO;
	using System.Threading;
	using System.Threading.Tasks;

	using Raven.Storage.Building;
	using Raven.Storage.Data;
	using Raven.Storage.Memtable;
	using Raven.Storage.Reading;

	using Version = Raven.Storage.Impl.Version;

	public class Compactor
	{
		private readonly StorageState state;

		private readonly IList<ulong> pendingOutputs = new List<ulong>();

		public Compactor(StorageState state)
		{
			this.state = state;
		}

		internal void MaybeScheduleCompaction(AsyncLock.LockScope lockScope)
		{
			Debug.Assert(lockScope != null);
			if (state.BackgroundCompactionScheduled)
			{
				return; // alread scheduled, nothing to do
			}
			if (state.ShuttingDown)
			{
				return;    // DB is being disposed; no more background compactions
			}
			if (state.ImmutableMemTable == null &&
				state.ManualCompaction == null &&
				state.VersionSet.NeedsCompaction)
			{
				// No work to be done
				return;
			}
			state.BackgroundCompactionScheduled = true;
			state.BackgroundTask = Task.Factory.StartNew(RunCompaction);
		}

		private async void RunCompaction()
		{
			try
			{
				this.BackgroundCompaction();
			}
			catch (Exception)
			{
				// Wait a little bit before retrying background compaction in
				// case this is an environmental problem and we do not want to
				// chew up resources for failed compactions for the duration of
				// the problem.

				Thread.Sleep(1000000);
			}

			using (var locker = await state.Lock.LockAsync())
			{
				this.MaybeScheduleCompaction(locker);
			}
		}

		private void BackgroundCompaction()
		{
			if (state.ImmutableMemTable != null)
			{
				CompactMemTable();
				return;
			}

			var isManual = false;

			try
			{
				Compaction compaction;
				var manualEnd = new Slice();
				isManual = state.ManualCompaction != null;
				if (isManual)
				{
					var mCompaction = state.ManualCompaction;
					compaction = state.VersionSet.CompactRange(mCompaction.Level, mCompaction.Begin, mCompaction.End);
					mCompaction.Done = compaction == null;
					if (compaction != null)
					{
						manualEnd = compaction.GetInput(0, compaction.GetNumberOfInputFiles(0) - 1).LargestKey;
					}
				}
				else
				{
					compaction = state.VersionSet.PickCompaction();
				}

				if (compaction == null)
				{
					// Nothing to do
				}
				else if (!isManual && compaction.IsTrivialMove())
				{
					Debug.Assert(compaction.GetNumberOfInputFiles(0) == 0);
					var file = compaction.GetInput(0, 0);
					compaction.Edit.DeleteFile(compaction.Level, file.FileNumber);
					compaction.Edit.AddFile(compaction.Level + 1, file);

					state.LogAndApply(compaction.Edit);

					//	VersionSet::LevelSummaryStorage tmp;
					//  Log(options_.info_log, "Moved #%lld to level-%d %lld bytes %s: %s\n",
					//	static_cast<unsigned long long>(f->number),
					//	c->level() + 1,
					//	static_cast<unsigned long long>(f->file_size),
					//	status.ToString().c_str(),
					//	versions_->LevelSummary(&tmp));
				}
				else
				{
					var compactionState = new CompactionState(compaction);
					DoCompactionWork(compactionState);
					CleanupCompaction(compactionState);
					compaction.ReleaseInputs();
					DeleteObsoleteFiles();
				}

				if (isManual)
				{
					var mCompaction = state.ManualCompaction;
					if (!mCompaction.Done)
					{
						mCompaction.Begin = manualEnd;
					}
					else
					{
						state.ManualCompaction = null;
					}
				}
			}
			catch (Exception)
			{
				//		Log(options_.info_log,
				//			"Compaction error: %s", status.ToString().c_str());
				//		if (options_.paranoid_checks && bg_error_.ok())
				//		{
				//			bg_error_ = status;
				//		}

				if (isManual)
				{
					var mCompaction = state.ManualCompaction;
					mCompaction.Done = true;
				}
			}
		}

		private void CleanupCompaction(CompactionState compactionState)
		{
			foreach (var output in compactionState.Outputs)
			{
				this.pendingOutputs.Remove(output.FileNumber);
			}

			compactionState.Dispose();
		}

		private void DoCompactionWork(CompactionState compactionState)
		{
			var watch = Stopwatch.StartNew();

			//Log(options_.info_log, "Compacting %d@%d + %d@%d files",
			//  compact->compaction->num_input_files(0),
			//  compact->compaction->level(),
			//  compact->compaction->num_input_files(1),
			//  compact->compaction->level() + 1);

			Debug.Assert(state.VersionSet.GetNumberOfFilesAtLevel(compactionState.Compaction.Level) > 0);
			Debug.Assert(compactionState.Builder == null);
			Debug.Assert(compactionState.OutFile == null);

			//if (snapshots_.empty())
			//{
			compactionState.SmallestSnapshot = this.state.VersionSet.LastSequence;
			//}
			//else
			//{
			//	compact->smallest_snapshot = snapshots_.oldest()->number_;
			//}

			// Release mutex while we're actually doing the compaction work
			// mutex_.Unlock();

			IIterator input = state.VersionSet.MakeInputIterator(compactionState.Compaction);
			input.SeekToFirst();

			ParsedInternalKey internalKey;
			Slice currentUserKey = null;
			var lastSequenceForKey = Format.MaxSequenceNumber;
			for (; input.IsValid; )
			{
				if (this.state.ImmutableMemTable != null)
				{
					this.CompactMemTable();
					// bg_cv_.SignalAll();  // Wakeup MakeRoomForWrite() if necessary
				}

				var key = input.Key;
				if (compactionState.Compaction.ShouldStopBefore(key) && compactionState.Builder != null)
				{
					try
					{
						FinishCompactionOutputFile(compactionState, input);
					}
					catch (Exception)
					{
						break;
					}
				}

				if (!ParsedInternalKey.TryParseInternalKey(key, out internalKey))
				{
					currentUserKey = null;
					lastSequenceForKey = Format.MaxSequenceNumber;
				}
				else
				{
					bool drop = false;
					if (currentUserKey.IsEmpty() || state.InternalKeyComparator.UserComparator.Compare(internalKey.UserKey, currentUserKey) != 0)
					{
						// First occurrence of this user key
						currentUserKey = internalKey.UserKey;
						lastSequenceForKey = Format.MaxSequenceNumber;
					}

					if (lastSequenceForKey <= compactionState.SmallestSnapshot)
					{
						// Hidden by an newer entry for same user key
						drop = true;
					}
					else if (internalKey.Type == ItemType.Deletion && internalKey.Sequence <= compactionState.SmallestSnapshot
							 && compactionState.Compaction.IsBaseLevelForKey(internalKey.UserKey))
					{
						// For this user key:
						// (1) there is no data in higher levels
						// (2) data in lower levels will have larger sequence numbers
						// (3) data in layers that are being compacted here and have
						//     smaller sequence numbers will be dropped in the next
						//     few iterations of this loop (by rule (A) above).
						// Therefore this deletion marker is obsolete and can be dropped.

						drop = true;
					}

					lastSequenceForKey = internalKey.Sequence;

					if (!drop)
					{
						// Open output file if necessary
						if (compactionState.Builder == null)
						{
							try
							{
								OpenCompactionOutputFile(compactionState);
							}
							catch (Exception)
							{
								break;
							}
						}

						if (compactionState.Builder.NumEntries == 0)
						{
							//compact->current_output()->smallest.DecodeFrom(key);
						}

						//compact->current_output()->largest.DecodeFrom(key);
						compactionState.Builder.Add(key, input.CreateValueStream());

						// Close output file if it is big enoug
						if (compactionState.Builder.FileSize >= compactionState.Compaction.MaxOutputFileSize)
						{
							try
							{
								FinishCompactionOutputFile(compactionState, input);
							}
							catch (Exception)
							{
								break;
							}
						}
					}
				}

				input.Next();
			}

			if (compactionState.Builder != null)
			{
				FinishCompactionOutputFile(compactionState, input);
			}

			input.Dispose();

			var stats = new CompactionStats
			{
				Micros = watch.ElapsedMilliseconds
			};

			for (var which = 0; which < 2; which++)
			{
				for (var i = 0; i < compactionState.Compaction.GetNumberOfInputFiles(which); i++)
				{
					stats.BytesRead += compactionState.Compaction.GetInput(which, i).FileSize;
				}
			}

			foreach (var output in compactionState.Outputs)
			{
				stats.BytesWritten += output.FileSize;
			}

			state.CompactionStats[compactionState.Compaction.Level + 1].Add(stats);

			InstallCompactionResults(compactionState);
		}

		private void OpenCompactionOutputFile(CompactionState compactionState)
		{
			Debug.Assert(compactionState != null);
			Debug.Assert(compactionState.Builder != null);

			ulong fileNumber;
			// lock mutex

			fileNumber = this.state.VersionSet.NewFileNumber();
			pendingOutputs.Add(fileNumber);
			compactionState.AddOutput(fileNumber);

			//unlock mutex

			// make the output file
			var fileName = this.state.FileSystem.GetFileName(this.state.DatabaseName, fileNumber, Constants.Files.Extensions.TableFile);
			var tempFileName = this.state.FileSystem.GetFileName(this.state.DatabaseName, fileNumber, Constants.Files.Extensions.TempFile);

			var file = this.state.FileSystem.NewWritable(fileName);
			var tempFile = this.state.FileSystem.NewReadableWritable(tempFileName);

			compactionState.OutFile = file;
			compactionState.Builder = new TableBuilder(this.state.Options, file, () => tempFile);
		}

		private void InstallCompactionResults(CompactionState compactionState)
		{
			// mutex.AssertHeld();

			//Log(options_.info_log,  "Compacted %d@%d + %d@%d files => %lld bytes",
			//  compact->compaction->num_input_files(0),
			//  compact->compaction->level(),
			//  compact->compaction->num_input_files(1),
			//  compact->compaction->level() + 1,
			//  static_cast<long long>(compact->total_bytes));

			compactionState.Compaction.AddInputDeletions(compactionState.Compaction.Edit);
			var level = compactionState.Compaction.Level;
			foreach (var output in compactionState.Outputs)
			{
				compactionState.Compaction.Edit.AddFile(level + 1, output);
			}

			this.state.LogAndApply(compactionState.Compaction.Edit);
		}

		private void FinishCompactionOutputFile(CompactionState compactionState, IIterator input)
		{
			Debug.Assert(compactionState != null);
			Debug.Assert(compactionState.OutFile != null);
			Debug.Assert(compactionState.Builder != null);

			var outputNumber = compactionState.CurrentOutput.FileNumber;
			Debug.Assert(outputNumber != 0);

			var currentEntries = compactionState.Builder.NumEntries;
			//if (s.ok())
			//{
			//	s = compact->builder->Finish();
			//}
			//else
			//{
			//	compact->builder->Abandon();
			//}

			if (input.IsValid)
			{
				compactionState.Builder.Finish();
			}

			var currentBytes = compactionState.Builder.FileSize;
			compactionState.CurrentOutput.FileSize = currentBytes;
			compactionState.TotalBytes += currentBytes;

			compactionState.Builder.Dispose();
			compactionState.Builder = null;

			compactionState.OutFile.Flush();
			compactionState.OutFile.Close();

			compactionState.OutFile.Dispose();
			compactionState.OutFile = null;

			if (currentEntries > 0)
			{
				// Verify that the table is usable
				using (this.state.TableCache.NewIterator(new ReadOptions(), outputNumber, currentBytes))
				{
				}
			}
		}

		/// <summary>
		/// Compact the in-memory write buffer to disk.  Switches to a new
		/// log-file/memtable and writes a new descriptor if successful.
		/// </summary>
		private void CompactMemTable()
		{
			if (state.ImmutableMemTable == null)
				throw new InvalidOperationException("ImmutableMemTable cannot be null.");

			var immutableMemTable = state.ImmutableMemTable;

			var edit = new VersionEdit();
			var currentVersion = this.state.VersionSet.Current;

			WriteLevel0Table(immutableMemTable, edit, currentVersion);

			// Replace immutable memtable with the generated Table

			edit.SetPrevLogNumber(0);
			edit.SetLogNumber(state.LogFileNumber);
			this.state.LogAndApply(edit); // maybe add mutex?

			this.state.ImmutableMemTable = null;
			DeleteObsoleteFiles();
		}

		private void DeleteObsoleteFiles()
		{
			var live = pendingOutputs;
			state.VersionSet.AddLiveFiles(live);

			var databaseName = state.DatabaseName;
			var databaseFiles = new DirectoryInfo(databaseName).GetFiles();

			foreach (var file in databaseFiles)
			{
				ulong number;
				FileType fileType;
				if (state.FileSystem.TryParseDatabaseFile(file, out number, out fileType))
				{
					var keep = true;
					switch (fileType)
					{
						case FileType.LogFile:
							keep = ((number >= state.VersionSet.LogNumber) || (number == state.VersionSet.PrevLogNumber));
							break;
						case FileType.DescriptorFile:
							// Keep my manifest file, and any newer incarnations'
							// (in case there is a race that allows other incarnations)
							keep = (number >= state.VersionSet.ManifestFileNumber);
							break;
						case FileType.TableFile:
							keep = (live.IndexOf(number) != live.Count - 1);
							break;
						case FileType.TempFile:
							// Any temp files that are currently being written to must
							// be recorded in pending_outputs_, which is inserted into "live"
							keep = (live.IndexOf(number) != live.Count - 1);
							break;
						case FileType.CurrentFile:
						case FileType.DBLockFile:
						case FileType.InfoLogFile:
							break;
						default:
							throw new NotSupportedException(fileType.ToString());
					}

					if (!keep)
					{
						if (fileType == FileType.TableFile)
						{
							state.TableCache.Evict(number);
						}

						//Log(options_.info_log, "Delete type=%d #%lld\n",
						//int(type),
						//static_cast<unsigned long long>(number));

						File.Delete(string.Format("{0}/{1}", databaseName, file));
					}
				}
			}
		}

		private void WriteLevel0Table(MemTable memTable, VersionEdit edit, Version currentVersion)
		{
			var stopwatch = Stopwatch.StartNew();
			var fileNumber = this.state.VersionSet.NewFileNumber();

			pendingOutputs.Add(fileNumber);

			var fileMetadata = state.BuildTable(memTable, fileNumber);

			pendingOutputs.Remove(fileNumber);

			// Note that if file_size is zero, the file has been deleted and
			// should not be added to the manifest.
			int level = 0;
			if (fileMetadata.FileSize > 0)
			{
				var smallestKey = fileMetadata.SmallestKey;
				var largestKey = fileMetadata.LargestKey;

				if (currentVersion != null)
				{
					level = currentVersion.PickLevelForMemTableOutput(smallestKey, largestKey);
				}

				edit.AddFile(level, fileMetadata);
			}

			state.CompactionStats[level].Add(new CompactionStats
			{
				Micros = stopwatch.ElapsedMilliseconds,
				BytesRead = 0,
				BytesWritten = fileMetadata.FileSize
			});
		}
	}
}