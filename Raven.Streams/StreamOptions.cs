using System;
using System.IO;
using System.Threading;
using Raven.Abstractions.Json;
using Raven.Abstractions.Logging;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;
using Raven.Storage;
using Raven.Storage.Comparing;
using Raven.Storage.Data;
using Raven.Storage.Filtering;

namespace Raven.Streams
{
	public class StreamOptions : StorageOptions, IDisposable
	{
		private static readonly ILog logger = LogManager.GetCurrentClassLogger();

		public int MaxInMemoryBuffer { get; set; }
		public int InitialInMemoryBuffer { get; set; }
		public LowLevelStorage Storage { get; set; }
		public CurrentStatus Status { get; set; }

		public StreamOptions()
		{
			MaxInMemoryBuffer = 1024 * 1024 * 256;
			InitialInMemoryBuffer = 1024 * 1024 * 4;
			Comparator = new ByteWiseComparator();
			FilterPolicy = new BloomFilterPolicy(caseInsensitive: false);
			Storage = new InMemoryLowLevelStorage(BufferPool);
		}

		public void Initialize()
		{
			long version;
			if (TryGetLatesttatusFileName(out version) == false)
			{
				Status = new CurrentStatus
					{
						EtagBase = 1,
						FileBase = 0,
						NextSeq = 0
					};
			}
			else
			{
				using (var stream = Storage.Read(StatusFileName(version)))
				using (var streamReader = new StreamReader(stream))
				{
					Status = JsonSerializer.Deserialize<CurrentStatus>(new JsonTextReader(streamReader));
					Status.EtagBase++;
				}
			}
		}

		private bool TryGetLatesttatusFileName(out long version)
		{
			if (Storage.Exists("status.current"))
			{
				try
				{
					using (var stream = Storage.Read("status.current"))
					using (var streamReader = new StreamReader(stream))
					{
						var versionStr = streamReader.ReadLine();
						version = long.Parse(versionStr);
						return true;
					}
				}
				catch (Exception e)
				{
					logger.WarnException("Could not read status.current file, will try finding latest status", e);
				}
			}
			bool hasMatch = false;
			version = -1;
			// because we read them in order, the last one is the latest
			foreach (var fileName in Storage.GetFileNamesInOrder())
			{
				if(fileName.StartsWith("status.", StringComparison.InvariantCulture) == false)
					continue;

				var justVersionStr = fileName.Substring("status.".Length);
				long result;
				if (long.TryParse(justVersionStr, out result))
				{
					hasMatch = true;
					version = result;
				}
			}
			return hasMatch;
		}


		public Stream CreateNewTableFile(out string fileName)
		{
			fileName = Status.CreateNewTableFileName();
			return Storage.Create(fileName);
		}

		public void Dispose()
		{

		}

		// REQUIRE: External syncronization for Status
		public void FlushStatus()
		{
			var oldVersion = Status.Version;
			Status.Version++;
			using (var file = Storage.Create(StatusFileName(Status.Version)))
			{
				var streamWriter = new StreamWriter(file);
				JsonSerializer.Serialize(streamWriter, Status);
				streamWriter.Flush();
				Storage.Flush(file);
			}

			using (var file = Storage.Create("status.current"))
			using (var streamWriter = new StreamWriter(file))
			{
				streamWriter.Write(Status.Version);
				Storage.Flush(file);
			}

			var statusFileName = StatusFileName(oldVersion);
			Storage.Delete(statusFileName);
		}

		private static JsonSerializer JsonSerializer
		{
			get
			{
				return new JsonSerializer
					{
						Converters = {new SliceJsonConverter()},
						Formatting = Formatting.Indented
					};
			}
		}

		private string StatusFileName(long version)
		{
			return string.Format("status.{0:00000000}", version);
		}

		public Stream CreateNewLogFile(out long log)
		{
			var file = Status.CreateNewLogFileName(out log);
			return Storage.Create(file);
		}
	}
}