using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;

using Raven.Abstractions.Logging;
using Raven.Storage.Building;
using Raven.Storage.Data;
using Raven.Storage.Memtable;
using Raven.Storage.Reading;

namespace Raven.Storage.Impl.Compactions
{
	public class Compactor
	{
		private readonly ILog log = LogManager.GetCurrentClassLogger();

		private readonly StorageState state;

		private readonly IList<ulong> pendingOutputs = new List<ulong>();

		private ManualCompaction manualCompaction;

		public Compactor(StorageState state)
		{
			this.state = state;
		}

		internal Task CompactAsync(int level, Slice begin, Slice end, AsyncLock locker)
		{
			if (manualCompaction != null)
				throw new InvalidOperationException("Manual compaction is already in progess.");

			manualCompaction = new ManualCompaction(level, new InternalKey(begin, Format.MaxSequenceNumber, ItemType.ValueForSeek), new InternalKey(end, Format.MaxSequenceNumber, ItemType.ValueForSeek));

			return Task.Factory.StartNew(async () =>
					{
						while (true)
						{
							if (state.ShuttingDown)
								throw new InvalidOperationException("Database is shutting down.");

							if (state.BackgroundCompactionScheduled)
							{
								await Task.Delay(100);
								continue;
							}

							Task task;
							using (var actualLock = await locker.LockAsync())
							{
								MaybeScheduleCompactionAsync(actualLock);
								task = state.BackgroundTask;
							}
							await task;
						}
					});
		}

		internal void MaybeScheduleCompactionAsync(AsyncLock.LockScope locker)
		{
			Debug.Assert(locker != null && locker.Locked);

			if (state.BackgroundCompactionScheduled)
			{
				return; // alread scheduled, nothing to do
			}
			if (state.ShuttingDown)
			{
				return;    // DB is being disposed; no more background compactions
			}
			if (state.ImmutableMemTable == null &&
				manualCompaction == null &&
				state.VersionSet.NeedsCompaction == false)
			{
				// No work to be done
				return;
			}

			state.BackgroundCompactionScheduled = true;
			state.BackgroundTask = Task.Factory.StartNew(() => RunCompactionAsync());
		}

		private async Task RunCompactionAsync()
		{
			using (LogManager.OpenMappedContext("storage", state.DatabaseName))
			using (var locker = await state.Lock.LockAsync())
			{
				try
				{
					bool needToWait = false;
					try
					{
						await BackgroundCompactionAsync(locker);
					}
					catch (Exception e)
					{
						log.ErrorException(string.Format("Compaction error: {0}", e.Message), e);

						state.BackgroundCompactionScheduled = false;
						needToWait = true;
					}

					// Wait a little bit before retrying background compaction in
					// case this is an environmental problem and we do not want to
					// chew up resources for failed compactions for the duration of
					// the problem.
					if (needToWait)
						await Task.Delay(1000);
				}
				finally
				{
					state.BackgroundCompactionScheduled = false;
				}

				MaybeScheduleCompactionAsync(locker);
			}
		}

		private async Task BackgroundCompactionAsync(AsyncLock.LockScope locker)
		{
			if (state.ImmutableMemTable != null)
			{
				await CompactMemTableAsync(locker);
				return;
			}

			try
			{
				Compaction compaction;
				var manualEnd = new InternalKey();
				if (manualCompaction != null)
				{
					var mCompaction = manualCompaction;
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
				else if (manualCompaction == null && compaction.IsTrivialMove())
				{
					Debug.Assert(compaction.GetNumberOfInputFiles(0) == 0);
					var file = compaction.GetInput(0, 0);
					compaction.Edit.DeleteFile(compaction.Level, file.FileNumber);
					compaction.Edit.AddFile(compaction.Level + 1, file);

					await state.LogAndApplyAsync(compaction.Edit, locker);

					log.Info("Moved {0} to level-{1} {2} bytes", file.FileNumber, compaction.Level + 1, file.FileSize);
				}
				else
				{
					var compactionState = new CompactionState(compaction);
					await DoCompactionWorkAsync(compactionState, locker);
					CleanupCompaction(compactionState);
					compaction.ReleaseInputs();
					DeleteObsoleteFiles();
				}

				if (manualCompaction != null)
				{
					var mCompaction = manualCompaction;
					if (!mCompaction.Done)
					{
						mCompaction.Begin = manualEnd;
					}
					else
					{
						manualCompaction = null;
					}
				}
			}
			catch (Exception)
			{
				if (manualCompaction != null)
				{
					var mCompaction = manualCompaction;
					mCompaction.Done = true;
				}

				throw;
			}
		}

		private void CleanupCompaction(CompactionState compactionState)
		{
			foreach (var output in compactionState.Outputs)
			{
				pendingOutputs.Remove(output.FileNumber);
			}

			compactionState.Dispose();
		}

		private async Task DoCompactionWorkAsync(CompactionState compactionState, AsyncLock.LockScope locker)
		{
			await locker.LockAsync();
			var watch = Stopwatch.StartNew();

			log.Info("Compacting {0}@{1} + {2}@{3} files.", compactionState.Compaction.GetNumberOfInputFiles(0), compactionState.Compaction.Level, compactionState.Compaction.GetNumberOfInputFiles(1), compactionState.Compaction.Level + 1);

			Debug.Assert(state.VersionSet.GetNumberOfFilesAtLevel(compactionState.Compaction.Level) > 0);
			Debug.Assert(compactionState.Builder == null);
			Debug.Assert(compactionState.OutFile == null);

			compactionState.SmallestSnapshot = state.Snapshooter.Snapshots.Count == 0 ? state.VersionSet.LastSequence : state.Snapshooter.Snapshots.First().Sequence;

			// Release mutex while we're actually doing the compaction work
			locker.Exit();

			IIterator input = state.VersionSet.MakeInputIterator(compactionState.Compaction);
			input.SeekToFirst();

			Slice currentUserKey = null;
			var lastSequenceForKey = Format.MaxSequenceNumber;
			for (; input.IsValid; )
			{
				if (state.ImmutableMemTable != null)
				{
					await CompactMemTableAsync(locker);
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

				InternalKey internalKey;
				if (!InternalKey.TryParse(key, out internalKey))
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
								await OpenCompactionOutputFileAsync(compactionState, locker);
								Debug.Assert(compactionState.Builder != null);
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

			await InstallCompactionResultsAsync(compactionState, locker);
		}

		private async Task OpenCompactionOutputFileAsync(CompactionState compactionState, AsyncLock.LockScope locker)
		{
			Debug.Assert(compactionState != null);
			Debug.Assert(compactionState.Builder != null);

			await locker.LockAsync();

			var fileNumber = state.VersionSet.NewFileNumber();
			pendingOutputs.Add(fileNumber);
			compactionState.AddOutput(fileNumber);

			locker.Exit();

			// make the output file
			var fileName = state.FileSystem.GetTableFileName(fileNumber);
			var file = state.FileSystem.NewWritable(fileName);

			compactionState.OutFile = file;
			compactionState.Builder = new TableBuilder(state.Options, file, () => state.FileSystem.NewReadableWritable(state.FileSystem.GetTempFileName(fileNumber)));
		}

		private async Task InstallCompactionResultsAsync(CompactionState compactionState, AsyncLock.LockScope locker)
		{
			await locker.LockAsync();

			log.Info("Compacted {0}@{1} + {2}@{3} files => {4} bytes", compactionState.Compaction.GetNumberOfInputFiles(0), compactionState.Compaction.Level, compactionState.Compaction.GetNumberOfInputFiles(1), compactionState.Compaction.Level + 1, compactionState.TotalBytes);

			compactionState.Compaction.AddInputDeletions(compactionState.Compaction.Edit);
			var level = compactionState.Compaction.Level;
			foreach (var output in compactionState.Outputs)
			{
				compactionState.Compaction.Edit.AddFile(level + 1, output);
			}

			await state.LogAndApplyAsync(compactionState.Compaction.Edit, locker);
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
				using (state.TableCache.NewIterator(new ReadOptions(), outputNumber, currentBytes))
				{
				}
			}
		}

		/// <summary>
		/// Compact the in-memory write buffer to disk.  Switches to a new
		/// log-file/memtable and writes a new descriptor if successful.
		/// </summary>
		/// <param name="locker"></param>
		private async Task CompactMemTableAsync(AsyncLock.LockScope locker)
		{
			if (state.ImmutableMemTable == null)
				throw new InvalidOperationException("ImmutableMemTable cannot be null.");

			var immutableMemTable = state.ImmutableMemTable;

			var edit = new VersionEdit();
			var currentVersion = state.VersionSet.Current;

			WriteLevel0Table(immutableMemTable, currentVersion, ref edit);

			// Replace immutable memtable with the generated Table

			edit.SetPrevLogNumber(0);
			edit.SetLogNumber(state.LogFileNumber);
			await state.LogAndApplyAsync(edit, locker);

			state.ImmutableMemTable = null;
			DeleteObsoleteFiles();
		}

		internal void DeleteObsoleteFiles()
		{
			var live = new List<ulong>(pendingOutputs);
			var liveFiles = state.VersionSet.GetLiveFiles();
			live.AddRange(liveFiles);

			var databaseFiles = state.FileSystem.GetFiles();

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
							keep = live.Contains(number);
							break;
						case FileType.TempFile:
							// Any temp files that are currently being written to must
							// be recorded in pending_outputs_, which is inserted into "live"
							keep = live.Contains(number);
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

						log.Info("Delete type={0} {1}", fileType, number);

						state.FileSystem.DeleteFile(file.Name);
					}
				}
			}
		}

		public void WriteLevel0Table(MemTable memTable, Version currentVersion, ref VersionEdit edit)
		{
			var stopwatch = Stopwatch.StartNew();
			var fileNumber = state.VersionSet.NewFileNumber();

			pendingOutputs.Add(fileNumber);

			var fileMetadata = state.BuildTable(memTable, fileNumber);

			pendingOutputs.Remove(fileNumber);

			// Note that if file_size is zero, the file has been deleted and
			// should not be added to the manifest.
			int level = 0;
			if (fileMetadata.FileSize > 0)
			{
				var minUserKey = fileMetadata.SmallestKey.UserKey;
				var maxUserKey = fileMetadata.LargestKey.UserKey;

				if (currentVersion != null)
				{
					level = currentVersion.PickLevelForMemTableOutput(minUserKey, maxUserKey);
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

		public async Task CompactRangeAsync(Slice begin, Slice end)
		{
			using (await state.Lock.LockAsync())
			{
				int maxLevelWithFiles = 1;

				var @base = state.VersionSet.Current;
				for (var level = 1; level < Config.NumberOfLevels; level++)
				{
					if (@base.OverlapInLevel(level, begin, end))
					{
						maxLevelWithFiles = level;
					}
				}

				for (var level = 0; level < maxLevelWithFiles; level++)
				{
					await CompactAsync(level, begin, end, state.Lock);
				}
			}
		}
	}
}