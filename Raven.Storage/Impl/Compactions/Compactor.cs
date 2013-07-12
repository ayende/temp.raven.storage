using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Raven.Storage.Building;
using Raven.Storage.Data;
using Raven.Storage.Memtable;
using Raven.Storage.Reading;
using Raven.Temp.Logging;

namespace Raven.Storage.Impl.Compactions
{
	using System.IO;
	using System.Threading;

	using Raven.Storage.Impl.Caching;
	using Raven.Storage.Util;

	public abstract class Compactor
	{
		protected readonly ILog log = LogManager.GetCurrentClassLogger();

		protected readonly StorageState state;

		private readonly IList<ulong> pendingOutputs = new List<ulong>();

		private SemaphoreSlim semaphore = new SemaphoreSlim(1, 1);

		protected Compactor(StorageState state)
		{
			this.state = state;
		}

		protected abstract Compaction CompactionToProcess();

		protected abstract bool IsManual { get; }

		protected async Task ScheduleCompactionAsync()
		{
			using (var locker = await state.Lock.LockAsync().ConfigureAwait(false))
			{
				Background.Work(RunCompactionAsync(locker));
			}
		}

		protected Task RunCompactionAsync(AsyncLock.LockScope locker)
		{
			Debug.Assert(state.BackgroundCompactionScheduled);
			state.BackgroundTask = Task.Factory.StartNew(async () =>
				{
					using (LogManager.OpenMappedContext("storage", state.DatabaseName))
					using (await locker.LockAsync())
					{
						try
						{
							bool needToWait = false;
							try
							{
								await semaphore.WaitAsync();
								await BackgroundCompactionAsync(locker).ConfigureAwait(false);
							}
							catch (Exception e)
							{
								log.ErrorException(string.Format("Compaction error: {0}", e.Message), e);

								needToWait = true;
							}

							// Wait a little bit before retrying background compaction in
							// case this is an environmental problem and we do not want to
							// chew up resources for failed compactions for the duration of
							// the problem.
							if (needToWait)
							{
								locker.Exit();
								await Task.Delay(1000).ConfigureAwait(false);
								await locker.LockAsync().ConfigureAwait(false);
							}

							await BackgroundCompactionAsync(locker).ConfigureAwait(false);
						}
						finally
						{
							semaphore.Release();
							state.BackgroundCompactionScheduled = false;
						}
					}

					return 1; // make R# happy
				}, TaskCreationOptions.LongRunning).Unwrap();

			return state.BackgroundTask;
		}


		private async Task BackgroundCompactionAsync(AsyncLock.LockScope locker)
		{
			state.CancellationToken.ThrowIfCancellationRequested();

			if (state.ImmutableMemTable != null)
			{
				await CompactMemTableAsync(locker).ConfigureAwait(false);
			}

			var compaction = CompactionToProcess();

			if (compaction == null)
			{
				return;
			}

			if (IsManual == false && compaction.IsTrivialMove())
			{
				Debug.Assert(compaction.GetNumberOfInputFiles(0) == 1);
				var file = compaction.GetInput(0, 0);
				compaction.Edit.DeleteFile(compaction.Level, file.FileNumber);
				compaction.Edit.AddFile(compaction.Level + 1, file);

				await state.LogAndApplyAsync(compaction.Edit, locker).ConfigureAwait(false);

				log.Info("Moved {0} to level-{1} {2} bytes", file.FileNumber, compaction.Level + 1, file.FileSize);
			}
			else
			{
				using (var compactionState = new CompactionState(compaction))
				{
					try
					{
						await DoCompactionWorkAsync(compactionState, locker).ConfigureAwait(false);
					}
					finally
					{
						CleanupCompaction(compactionState);
					}
				}

				compaction.ReleaseInputs();
				DeleteObsoleteFiles();
			}
		}

		private void CleanupCompaction(CompactionState compactionState)
		{
			foreach (var output in compactionState.Outputs)
			{
				pendingOutputs.Remove(output.FileNumber);
			}
		}

		private async Task DoCompactionWorkAsync(CompactionState compactionState, AsyncLock.LockScope locker)
		{
			var watch = Stopwatch.StartNew();

			using (await locker.LockAsync())
			{
				log.Info("Compacting {0}@{1} + {2}@{3} files.", compactionState.Compaction.GetNumberOfInputFiles(0), compactionState.Compaction.Level, compactionState.Compaction.GetNumberOfInputFiles(1), compactionState.Compaction.Level + 1);

				Debug.Assert(state.VersionSet.GetNumberOfFilesAtLevel(compactionState.Compaction.Level) > 0);
				Debug.Assert(compactionState.Builder == null);

				compactionState.SmallestSnapshot = state.Snapshooter.Snapshots.Count == 0 ? state.VersionSet.LastSequence : state.Snapshooter.Snapshots.First().Sequence;
			}

			Slice currentUserKey = null;
			var lastSequenceForKey = Format.MaxSequenceNumber;

			state.CancellationToken.ThrowIfCancellationRequested();

			await PerformRapidCompactionAsync(compactionState, watch, locker);
			return;

			using (IIterator input = state.VersionSet.MakeInputIterator(compactionState.Compaction))
			{
				input.SeekToFirst();
				while (input.IsValid)
				{
					if (state.ImmutableMemTable != null)
						await CompactMemTableAsync(locker).ConfigureAwait(false);

					state.CancellationToken.ThrowIfCancellationRequested();
					var key = input.Key;

					FinishCompactionOutputFileIfNecessary(compactionState, input);

					InternalKey internalKey;
					if (!InternalKey.TryParse(key, out internalKey))
					{
						currentUserKey = null;
						lastSequenceForKey = Format.MaxSequenceNumber;
						input.Next();
						continue;
					}

					var drop = false;
					if (currentUserKey.IsEmpty()
						|| state.InternalKeyComparator.UserComparator.Compare(internalKey.UserKey, currentUserKey) != 0)
					{
						// First occurrence of this user key
						currentUserKey = internalKey.UserKey.Clone();
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
						if (compactionState.Builder == null)
						{
							using (await locker.LockAsync())
								OpenCompactionOutputFileIfNecessary(compactionState, locker);
						}

						Debug.Assert(compactionState.Builder != null);

						if (compactionState.Builder.NumEntries == 0)
							compactionState.CurrentOutput.SmallestKey = new InternalKey(key.Clone());

						compactionState.CurrentOutput.LargestKey = new InternalKey(key.Clone());

						using (var stream = input.CreateValueStream())
							compactionState.Builder.Add(key, stream);

						FinishCompactionOutputFileIfNecessary(compactionState, input);
					}

					input.Next();
				}

				FinishCompactionOutputFileIfNecessary(compactionState, input, force: true);
			}

			CreateCompactionStats(compactionState, watch);

			await InstallCompactionResultsAsync(compactionState, locker).ConfigureAwait(false);
		}

		private async Task PerformRapidCompactionAsync(CompactionState compactionState, Stopwatch watch, AsyncLock.LockScope locker)
		{
			using (var source = CreateInputIterator(compactionState))
			{
				InternalKey sourceInternalKey = new InternalKey();
				InternalKey fileInternalKey;
				FileMetadata file = null;
				IIterator fileIterator = null;

				Slice lastKey = null;
				ulong lastSequence = 0;
				bool addSource = true;

				try
				{
					var destinationFiles = compactionState.Compaction.Inputs[1].ToDictionary(x => x, x => true);

					source.SeekToFirst();
					while (source.IsValid)
					{
						if (state.ImmutableMemTable != null)
							await CompactMemTableAsync(locker).ConfigureAwait(false);

						state.CancellationToken.ThrowIfCancellationRequested();

						if (addSource && !InternalKey.TryParse(source.Key, out sourceInternalKey))
							throw new InvalidOperationException("Source key must be a valid internal key.");

						addSource = false;

						if (destinationFiles.Count > 0 && file == null)
						{
							file = PickOverlapingFile(sourceInternalKey, destinationFiles);
							CleanupIfNecessary(sourceInternalKey, compactionState, null, destinationFiles);
						}

						if (file != null && fileIterator == null)
						{
							fileIterator = CreateFileIterator(file);
							fileIterator.SeekToFirst();
						}

						if (fileIterator != null)
						{
							while (fileIterator.IsValid)
							{
								if (!InternalKey.TryParse(fileIterator.Key, out fileInternalKey))
									throw new InvalidOperationException("File key must be a valid internal key.");

								if (state.InternalKeyComparator.Compare(sourceInternalKey, fileInternalKey) <= 0)
								{
									addSource = true;
									break;
								}

								MaybeAddFile(fileInternalKey, source, compactionState, locker, ref lastKey, ref lastSequence);
								fileIterator.Next();
							}

							if (!fileIterator.IsValid)
							{
								file = null;
								fileIterator.Dispose();
								fileIterator = null;
							}
						}
						else
						{
							addSource = true;
						}

						if (!addSource)
							continue;

						MaybeAddFile(sourceInternalKey, source, compactionState, locker, ref lastKey, ref lastSequence);
						source.Next();
					}

					if (fileIterator != null)
					{
						while (fileIterator.IsValid)
						{
							if (!InternalKey.TryParse(fileIterator.Key, out fileInternalKey))
								throw new InvalidOperationException("File key must be a valid internal key.");

							MaybeAddFile(fileInternalKey, fileIterator, compactionState, locker, ref lastKey, ref lastSequence);

							fileIterator.Next();
						}
					}

					if (sourceInternalKey.UserKey.IsEmpty() == false)
						CleanupIfNecessary(sourceInternalKey, compactionState, null, destinationFiles);
				}
				finally
				{
					if (fileIterator != null)
						fileIterator.Dispose();
				}

				FinishCompactionOutputFileIfNecessary(compactionState, source, force: true);

				CreateCompactionStats(compactionState, watch);

				await InstallCompactionResultsAsync(compactionState, locker).ConfigureAwait(false);
			}
		}

		private void CleanupIfNecessary(InternalKey key, CompactionState compactionState, IIterator iterator, IDictionary<FileMetadata, bool> destinationFiles)
		{
			var filesToRemove = new List<KeyValuePair<FileMetadata, bool>>();
			foreach (var pair in destinationFiles)
			{
				var r = state.InternalKeyComparator.UserComparator.Compare(key.UserKey, pair.Key.LargestKey.UserKey);
				if (r > 0)
				{
					// past the file
					filesToRemove.Add(pair);
				}
				else if (r < 0)
				{
					// past the file - no need to check further
					break;
				}
			}

			if (filesToRemove.Count == 0)
				return;

			foreach (var pair in filesToRemove)
			{
				destinationFiles.Remove(pair);

				if (pair.Value)
					compactionState.Compaction.Inputs[1].Remove(pair.Key);
			}

			if (filesToRemove.Any(x => x.Value))
				FinishCompactionOutputFileIfNecessary(compactionState, iterator, true);
		}

		public void MaybeAddFile(InternalKey key, IIterator iterator, CompactionState compactionState, AsyncLock.LockScope locker, ref Slice lastKey, ref ulong lastSequence)
		{
			var drop = false;
			if (lastKey.IsEmpty()
				|| state.InternalKeyComparator.UserComparator.Compare(key.UserKey, lastKey) != 0)
			{
				// First occurrence of this user key
				lastKey = key.UserKey.Clone();
				lastSequence = Format.MaxSequenceNumber;
			}

			if (lastSequence <= compactionState.SmallestSnapshot)
			{
				// Hidden by an newer entry for same user key
				drop = true;
			}
			else if (key.Type == ItemType.Deletion && key.Sequence <= compactionState.SmallestSnapshot
					 && compactionState.Compaction.IsBaseLevelForKey(key.UserKey))
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

			lastSequence = key.Sequence;

			if (!drop)
			{
				if (compactionState.Builder == null)
				{
					using (locker.LockAsync().Result)
						OpenCompactionOutputFileIfNecessary(compactionState, locker);
				}

				Debug.Assert(compactionState.Builder != null);

				if (compactionState.Builder.NumEntries == 0)
					compactionState.CurrentOutput.SmallestKey = new InternalKey(key.TheInternalKey.Clone());

				compactionState.CurrentOutput.LargestKey = new InternalKey(key.TheInternalKey.Clone());

				//Console.WriteLine("Adding " + compactionState.CurrentOutput.LargestKey);

				using (var stream = iterator.CreateValueStream())
					compactionState.Builder.Add(key.TheInternalKey, stream);

				FinishCompactionOutputFileIfNecessary(compactionState, iterator);
			}
		}

		private IIterator CreateFileIterator(FileMetadata file)
		{
			var readOptions = new ReadOptions
			{
				VerifyChecksums = state.Options.ParanoidChecks,
				FillCache = false
			};

			return state.TableCache.NewIterator(readOptions, file.FileNumber, file.FileSize);
		}

		private FileMetadata PickOverlapingFile(InternalKey sourceKey, IDictionary<FileMetadata, bool> destinationFiles)
		{
			if (destinationFiles.Count == 0)
				return null;

			foreach (var pair in destinationFiles)
			{
				// is inside this file
				if (state.InternalKeyComparator.UserComparator.Compare(sourceKey.UserKey, pair.Key.SmallestKey.UserKey) >= 0
					&& state.InternalKeyComparator.UserComparator.Compare(sourceKey.UserKey, pair.Key.LargestKey.UserKey) <= 0)
				{
					destinationFiles[pair.Key] = false;
					return pair.Key;
				}
			}

			return null;
		}

		private IIterator CreateInputIterator(CompactionState compactionState)
		{
			var readOptions = new ReadOptions
			{
				VerifyChecksums = state.Options.ParanoidChecks,
				FillCache = false
			};

			return new MergingIterator(state.InternalKeyComparator, compactionState.Compaction.Inputs[0].Select(x => state.TableCache.NewIterator(readOptions, x.FileNumber, x.FileSize)).ToList());
		}

		private void CreateCompactionStats(CompactionState compactionState, Stopwatch watch)
		{
			var stats = new CompactionStats { Milliseconds = watch.ElapsedMilliseconds };

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
		}

		private void OpenCompactionOutputFileIfNecessary(CompactionState compactionState, AsyncLock.LockScope locker)
		{
			if (compactionState.Builder != null)
				return;

			Debug.Assert(locker.Locked);

			var fileNumber = this.state.VersionSet.NewFileNumber();
			pendingOutputs.Add(fileNumber);
			compactionState.AddOutput(fileNumber);

			locker.Exit();

			// make the output file
			var fileName = state.FileSystem.GetTableFileName(fileNumber);
			var file = state.FileSystem.NewWritable(fileName);

			compactionState.Builder = new TableBuilder(state, file, new TemporaryFiles(state.FileSystem, fileNumber));
		}

		private async Task InstallCompactionResultsAsync(CompactionState compactionState, AsyncLock.LockScope locker)
		{
			log.Info("Compacted {0}@{1} + {2}@{3} files => {4} bytes", compactionState.Compaction.GetNumberOfInputFiles(0), compactionState.Compaction.Level, compactionState.Compaction.GetNumberOfInputFiles(1), compactionState.Compaction.Level + 1, compactionState.TotalBytes);

			compactionState.Compaction.AddInputDeletions(compactionState.Compaction.Edit);
			var level = compactionState.Compaction.Level;
			foreach (var output in compactionState.Outputs)
				compactionState.Compaction.Edit.AddFile(level + 1, output);

			await state.LogAndApplyAsync(compactionState.Compaction.Edit, locker).ConfigureAwait(false);
		}

		private void FinishCompactionOutputFileIfNecessary(CompactionState compactionState, IIterator input, bool force = false)
		{
			if (compactionState.Builder == null)
				return;

			// Finish when:
			// 1. When close output file is big enough
			// 2. When we should stop before input key
			// 3. When forced
			if (compactionState.Builder.FileSize < compactionState.Compaction.MaxOutputFileSize
				&& force == false
				&& (input.IsValid == false || !compactionState.Compaction.ShouldStopBefore(input.Key)))
				return;

			//Console.WriteLine("Finishing");

			var outputNumber = compactionState.CurrentOutput.FileNumber;
			Debug.Assert(outputNumber != 0);

			var currentEntries = compactionState.Builder.NumEntries;

			compactionState.Builder.Finish();

			var currentBytes = compactionState.Builder.FileSize;
			compactionState.CurrentOutput.FileSize = currentBytes;
			compactionState.TotalBytes += currentBytes;

			compactionState.Builder.Dispose();
			compactionState.Builder = null;

			if (currentEntries <= 0)
				return;

			if (state.Options.ParanoidChecks)
				using (this.state.TableCache.NewIterator(new ReadOptions(), outputNumber, currentBytes))
				{
					// Verify that the table is usable
				}
		}

		/// <summary>
		/// Compact the in-memory write buffer to disk.  Switches to a new
		/// log-file/memtable and writes a new descriptor if successful.
		/// </summary>
		/// <param name="locker"></param>
		private async Task CompactMemTableAsync(AsyncLock.LockScope locker)
		{
			MemTable immutableMemTable;
			Version currentVersion;

			using (await locker.LockAsync())
			{
				if (state.ImmutableMemTable == null)
					throw new InvalidOperationException("ImmutableMemTable cannot be null.");

				immutableMemTable = state.ImmutableMemTable;
				currentVersion = state.VersionSet.Current;
			}

			state.CancellationToken.ThrowIfCancellationRequested();
			var edit = new VersionEdit();
			WriteLevel0Table(immutableMemTable, currentVersion, edit);

			// Replace immutable memtable with the generated Table

			edit.SetPrevLogNumber(0);
			edit.SetLogNumber(state.LogFileNumber);
			await state.LogAndApplyAsync(edit, locker).ConfigureAwait(false);

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
							state.TableCache.Evict(number);

						log.Info("Delete type={0} {1}", fileType, number);

						try
						{
							state.FileSystem.DeleteFile(file);
						}
						catch (IOException e)
						{
							log.ErrorException("Could not delete file: " + file, e);
						}
						catch (UnauthorizedAccessException e)
						{
							log.ErrorException("Could not delete file: " + file, e);
						}
					}
				}
			}
		}

		public void WriteLevel0Table(MemTable memTable, Version currentVersion, VersionEdit edit)
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
				Milliseconds = stopwatch.ElapsedMilliseconds,
				BytesRead = 0,
				BytesWritten = fileMetadata.FileSize
			});
		}
	}
}