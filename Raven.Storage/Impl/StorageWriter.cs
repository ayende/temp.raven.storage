using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;
using System.Linq;
using Raven.Storage.Memtable;

namespace Raven.Storage.Impl
{
	using System.Threading;

	using Raven.Storage.Comparing;
	using Raven.Storage.Data;
	using Raven.Storage.Reading;
	using Raven.Storage.Util;

	public class StorageWriter
	{
		private readonly StorageState _state;
		private readonly ConcurrentQueue<OutstandingWrite> _pendingWrites = new ConcurrentQueue<OutstandingWrite>();
		private readonly AsyncMonitor _writeCompletedEvent = new AsyncMonitor();

		private readonly IList<ulong> pendingOutputs = new List<ulong>();

		private class OutstandingWrite
		{
			public WriteBatch Batch { get; private set; }
			public TaskCompletionSource<object> Result { get; private set; }

			public OutstandingWrite(WriteBatch batch)
			{
				Batch = batch;
				Size = batch.Size;
				Result = new TaskCompletionSource<object>();
			}

			public long Size { get; private set; }

			public bool Done()
			{
				var task = Result.Task;
				if (task.IsCompleted)
					return true;
				if (task.IsCanceled || task.IsFaulted)
					task.Wait(); // throws
				return false;
			}
		}

		private readonly InternalKeyComparator internalKeyComparator;

		public StorageWriter(StorageState state)
		{
			_state = state;
			internalKeyComparator = new InternalKeyComparator(state.Options.Comparator);
		}

		public void Write(WriteBatch batch)
		{
			WriteAsync(batch).Wait();
		}

		public async Task WriteAsync(WriteBatch batch)
		{
			var mine = new OutstandingWrite(batch);
			_pendingWrites.Enqueue(mine);

			while (mine.Done() == false && _pendingWrites.Peek() != mine)
			{
				await _writeCompletedEvent.WaitAsync();
			}

			if (mine.Done())
				return;

			using (var locker = await _state.Lock.LockAsync())
			{
				try
				{
					if (mine.Done())
						return;

					await MakeRoomForWrite(force: false, lockScope: locker);

					var lastSequence = _state.VersionSet.LastSequence;

					var list = BuildBatchGroup(mine);

					var currentSequence = lastSequence + 1;

					lastSequence += (ulong)list.Count;

					// Add to log and apply to memtable.  We can release the lock
					// during this phase since mine is currently responsible for logging
					// and protects against concurrent loggers and concurrent writes
					// into the mem table.

					locker.Exit();
					{
						foreach (var write in list)
						{
							write.Batch.Prepare(_state.MemTable);
						}

						await WriteBatch.WriteToLog(list.Select(x => x.Batch).ToArray(), currentSequence, _state);

						foreach (var write in list)
						{
							write.Batch.Apply(_state.MemTable, currentSequence);
						}
					}
					await locker.LockAsync();
					_state.VersionSet.LastSequence = lastSequence;

					foreach (var outstandingWrite in list)
					{
						// notify items we already worked on...
						outstandingWrite.Result.SetResult(null);
					}
				}
				finally
				{
					_writeCompletedEvent.Pulse();
				}
			}
		}

		private async Task MakeRoomForWrite(bool force, AsyncLock.LockScope lockScope)
		{
			bool allowDelay = force == false;
			while (true)
			{
				if (_state.BackgroundTask.IsCanceled || _state.BackgroundTask.IsFaulted)
				{
					await _state.BackgroundTask;// throws
				}
				else if (allowDelay && _state.VersionSet.GetNumberOfFilesAtLevel(0) >= Config.SlowdownWritesTrigger)
				{
					// We are getting close to hitting a hard limit on the number of
					// L0 files.  Rather than delaying a single write by several
					// seconds when we hit the hard limit, start delaying each
					// individual write by 1ms to reduce latency variance.  Also,
					// this delay hands over some CPU to the compaction thread in
					// case it is sharing the same core as the writer.
					lockScope.Exit();
					{
						await Task.Delay(TimeSpan.FromMilliseconds(1));
					}
					await lockScope.LockAsync();
					allowDelay = false; // Do not delay a single write more than once
				}
				else if (force == false && _state.MemTable.ApproximateMemoryUsage <= _state.Options.WriteBatchSize)
				{
					// There is room in current memtable
					break;
				}
				else if (_state.ImmutableMemTable != null)
				{
					// We have filled up the current memtable, but the previous
					// one is still being compacted, so we wait.
					await _state.BackgroundTask;
				}
				else if (_state.VersionSet.GetNumberOfFilesAtLevel(0) >= Config.StopWritesTrigger)
				{
					// There are too many level-0 files.
					await _state.BackgroundTask;
				}
				else
				{
					// Attempt to switch to a new memtable and trigger compaction of old
					Debug.Assert(_state.VersionSet.PrevLogNumber == 0);

					_state.LogWriter.Dispose();

					_state.CreateNewLog();
					_state.ImmutableMemTable = _state.MemTable;
					_state.MemTable = new MemTable(_state.Options);
					force = false;
					MaybeScheduleCompaction(lockScope);
				}
			}
		}

		private void MaybeScheduleCompaction(AsyncLock.LockScope lockScope)
		{
			Debug.Assert(lockScope != null);
			if (_state.BackgroundCompactionScheduled)
			{
				return; // alread scheduled, nothing to do
			}
			if (_state.ShuttingDown)
			{
				return;    // DB is being disposed; no more background compactions
			}
			if (_state.ImmutableMemTable == null &&
				_state.ManualCompaction == null &&
				_state.VersionSet.NeedsCompaction)
			{
				// No work to be done
				return;
			}
			_state.BackgroundCompactionScheduled = true;
			_state.BackgroundTask = Task.Factory.StartNew(RunCompaction);
		}

		private async void RunCompaction()
		{
			var status = this.BackgroundCompaction();
			if (status.IsOK())
			{
				// Success
			}
			else
			{
				// Wait a little bit before retrying background compaction in
				// case this is an environmental problem and we do not want to
				// chew up resources for failed compactions for the duration of
				// the problem.

				Thread.Sleep(1000000);
			}

			using (var locker = await _state.Lock.LockAsync())
			{
				this.MaybeScheduleCompaction(locker);
			}
		}

		private Status BackgroundCompaction()
		{
			if (_state.ImmutableMemTable != null)
			{
				return CompactMemTable();
			}

			Compaction compaction;
			Slice manualEnd = new Slice();
			var isManual = _state.ManualCompaction != null;
			if (isManual)
			{
				var mCompaction = _state.ManualCompaction;
				compaction = _state.VersionSet.CompactRange(mCompaction.Level, mCompaction.Begin, mCompaction.End);
				mCompaction.Done = compaction == null;
				if (compaction != null)
				{
					manualEnd = compaction.GetInput(0, compaction.GetNumberOfInputFiles(0) - 1).LargestKey;
				}
			}
			else
			{
				compaction = _state.VersionSet.PickCompaction();
			}

			Status status = Status.OK();
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

				status = _state.LogAndApply(compaction.Edit);

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
				CompactionState compactionState = new CompactionState(compaction);
				status = DoCompactionWork(compactionState);
				CleanupCompaction(compactionState);
				compaction.ReleaseInputs();
				DeleteObsoleteFiles();
			}

			compaction = null;

			if (status.IsOK())
			{
				// DONE
			}
			else
			{
				//		Log(options_.info_log,
				//			"Compaction error: %s", status.ToString().c_str());
				//		if (options_.paranoid_checks && bg_error_.ok())
				//		{
				//			bg_error_ = status;
				//		}
			}

			if (isManual)
			{
				var mCompaction = _state.ManualCompaction;
				if (!status.IsOK())
				{
					mCompaction.Done = true;
				}

				if (!mCompaction.Done)
				{
					mCompaction.Begin = manualEnd;
				}
				else
				{
					_state.ManualCompaction = null;
				}
			}

			return status;
		}

		private void CleanupCompaction(CompactionState compactionState)
		{
			foreach (var output in compactionState.Outputs)
			{
				this.pendingOutputs.Remove(output.FileNumber);
			}

			compactionState.Dispose();
		}

		private Status DoCompactionWork(CompactionState compactionState)
		{
			var watch = Stopwatch.StartNew();

			//Log(options_.info_log, "Compacting %d@%d + %d@%d files",
			//  compact->compaction->num_input_files(0),
			//  compact->compaction->level(),
			//  compact->compaction->num_input_files(1),
			//  compact->compaction->level() + 1);

			Debug.Assert(_state.VersionSet.GetNumberOfFilesAtLevel(compactionState.Compaction.Level) > 0);
			Debug.Assert(compactionState.Builder == null);
			Debug.Assert(compactionState.OutFile == null);

			//if (snapshots_.empty())
			//{
			//	compact->smallest_snapshot = versions_->LastSequence();
			//}
			//else
			//{
			//	compact->smallest_snapshot = snapshots_.oldest()->number_;
			//}

			// Release mutex while we're actually doing the compaction work
			// mutex_.Unlock();

			IIterator input = _state.VersionSet.MakeInputIterator(compactionState.Compaction);
			input.SeekToFirst();

			Status status = Status.OK();
			ParsedInternalKey internalKey;
			Slice currentUserKey = null;
			var lastSequenceForKey = Format.MaxSequenceNumber;
			for (; input.IsValid; )
			{
				if (this._state.ImmutableMemTable != null)
				{
					this.CompactMemTable();
					// bg_cv_.SignalAll();  // Wakeup MakeRoomForWrite() if necessary
				}

				var key = input.Key;
				if (compactionState.Compaction.ShouldStopBefore(key) && compactionState.Builder != null)
				{
					status = FinishCompactionOutputFile(compactionState, input);
					if (!status.IsOK())
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
					if (currentUserKey.IsEmpty() || internalKeyComparator.UserComparator.Compare(internalKey.UserKey, currentUserKey) != 0)
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
							status = OpenCompactionOutputFile(compactionState);
							if (!status.IsOK())
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
							status = FinishCompactionOutputFile(compactionState, input);
							if (!status.IsOK())
							{
								break;
							}
						}
					}
				}

				input.Next();
			}

			if (status.IsOK() && compactionState.Builder != null)
			{
				status = FinishCompactionOutputFile(compactionState, input);
			}

			input.Dispose();
			input = null;

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

			_state.CompactionStats[compactionState.Compaction.Level + 1].Add(stats);

			if (status.IsOK())
			{
				status = InstallCompactionResults(compactionState);
			}

			return status;
		}

		private Status OpenCompactionOutputFile(CompactionState compactionState)
		{
			throw new NotImplementedException();
		}

		private Status InstallCompactionResults(CompactionState compactionState)
		{
			throw new NotImplementedException();
		}

		private Status FinishCompactionOutputFile(CompactionState compactionState, IIterator input)
		{
			throw new NotImplementedException();
		}

		/// <summary>
		/// Compact the in-memory write buffer to disk.  Switches to a new
		/// log-file/memtable and writes a new descriptor if successful.
		/// </summary>
		private Status CompactMemTable()
		{
			if (_state.ImmutableMemTable == null)
				throw new InvalidOperationException("ImmutableMemTable cannot be null.");

			var immutableMemTable = _state.ImmutableMemTable;

			VersionEdit edit = new VersionEdit();
			Version currentVersion = this._state.VersionSet.Current;

			Status status = WriteLevel0Table(immutableMemTable, edit, currentVersion);

			// Replace immutable memtable with the generated Table
			if (status.IsOK())
			{
				edit.SetPrevLogNumber(0);
				edit.SetLogNumber(_state.LogFileNumber);
				status = this._state.LogAndApply(edit); // maybe add mutex?
			}

			if (status.IsOK())
			{
				this._state.ImmutableMemTable = null;
				DeleteObsoleteFiles();
			}

			return status;
		}

		private void DeleteObsoleteFiles()
		{
			var live = pendingOutputs;
			_state.VersionSet.AddLiveFiles(live);

			var databaseName = _state.DatabaseName;
			var databaseFiles = new DirectoryInfo(databaseName).GetFiles();

			foreach (var file in databaseFiles)
			{
				ulong number;
				FileType fileType;
				if (_state.FileSystem.TryParseDatabaseFile(file, out number, out fileType))
				{
					var keep = true;
					switch (fileType)
					{
						case FileType.LogFile:
							keep = ((number >= _state.VersionSet.LogNumber) || (number == _state.VersionSet.PrevLogNumber));
							break;
						case FileType.DescriptorFile:
							// Keep my manifest file, and any newer incarnations'
							// (in case there is a race that allows other incarnations)
							keep = (number >= _state.VersionSet.ManifestFileNumber);
							break;
						case FileType.TableFile:
							keep = (live.IndexOf(number) != live.Count() - 1);
							break;
						case FileType.TempFile:
							// Any temp files that are currently being written to must
							// be recorded in pending_outputs_, which is inserted into "live"
							keep = (live.IndexOf(number) != live.Count() - 1);
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
							//table_cache_->Evict(number);
						}

						//Log(options_.info_log, "Delete type=%d #%lld\n",
						//int(type),
						//static_cast<unsigned long long>(number));

						File.Delete(string.Format("{0}/{1}", databaseName, file));
					}
				}
			}
		}

		private Status WriteLevel0Table(MemTable memTable, VersionEdit edit, Version currentVersion)
		{
			var stopwatch = Stopwatch.StartNew();
			var fileNumber = this._state.VersionSet.NewFileNumber();

			pendingOutputs.Add(fileNumber);

			var fileMetadata = _state.BuildTable(memTable, fileNumber);

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

			_state.CompactionStats[level].Add(new CompactionStats
												  {
													  Micros = stopwatch.ElapsedMilliseconds,
													  BytesRead = 0,
													  BytesWritten = fileMetadata.FileSize
												  });

			return Status.OK();
		}

		private List<OutstandingWrite> BuildBatchGroup(OutstandingWrite mine)
		{
			// Allow the group to grow up to a maximum size, but if the
			// original write is small, limit the growth so we do not slow
			// down the small write too much.
			long maxSize = 1024 * 1024; // 1 MB by default
			if (mine.Size < 128 * 1024)
				maxSize = mine.Size + (128 * 1024);

			var list = new List<OutstandingWrite> { mine };
			foreach (var item in _pendingWrites.Skip(1)) //skip the first since we already added it
			{
				maxSize -= item.Size;
				if (maxSize < 0)
					break;
				list.Add(item);
			}
			return list;
		}
	}
}