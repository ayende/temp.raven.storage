using System;
using System.Collections.Generic;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Text.RegularExpressions;
using Raven.Storage.Memory;
using Raven.Storage.Util;
using System.Linq;

namespace Raven.Storage.Impl
{
	public class FileSystem : IDisposable
	{
		private readonly string databaseName;
		private FileStream lockFile;

		public FileSystem(string databaseName)
		{
			this.databaseName = databaseName;
		}

		public string GetFileName(string name, ulong num, string ext)
		{
			return string.Format("{0}{1:000000}{2}", name, num, ext);
		}

		public string GetFullFileName(string name, ulong num, string ext)
		{
			return Path.Combine(databaseName, string.Format("{0}{1:000000}{2}", name, num, ext));
		}

		public virtual string GetFullFileName(ulong num, string ext)
		{
			return Path.Combine(databaseName, string.Format("{0:000000}{1}", num, ext));
		}

		public virtual Stream NewWritable(string name)
		{
			var stream = File.Open(Path.Combine(databaseName, name), FileMode.CreateNew, FileAccess.ReadWrite);
			TrackResourceUsage.Track(() => stream.SafeFileHandle);
			return stream;
		}
		
		public Stream NewWritable(ulong num, string ext)
		{
			return NewWritable(GetFileName(string.Empty, num, ext));
		}

		public virtual void DeleteFile(string name)
		{
			if (File.Exists(Path.Combine(databaseName, name)))
				File.Delete(Path.Combine(databaseName, name));
		}

		public bool TryParseDatabaseFile(string file, out ulong number, out FileType fileType)
		{
			number = 0;
			fileType = FileType.Unknown;

			var fileName = ExtractFileName(file);

			if (fileName.Equals(Constants.Files.CurrentFile, StringComparison.InvariantCultureIgnoreCase))
			{
				number = 0;
				fileType = FileType.CurrentFile;
			}
			else if (fileName.Equals(Constants.Files.DBLockFile, StringComparison.InvariantCultureIgnoreCase))
			{
				number = 0;
				fileType = FileType.DBLockFile;
			}
			else if (fileName.Equals(Constants.Files.LogFile) || file.Equals(Constants.Files.CurrentFile + ".old"))
			{
				number = 0;
				fileType = FileType.InfoLogFile;
			}
			else if (fileName.StartsWith(Constants.Files.ManifestPrefix, StringComparison.InvariantCultureIgnoreCase))
			{
				if (!string.IsNullOrEmpty(Path.GetExtension(file)))
				{
					return false;
				}

				var prefixLength = Constants.Files.ManifestPrefix.Length;
				if (!ulong.TryParse(fileName.Substring(prefixLength, file.Length - prefixLength), out number))
				{
					return false;
				}

				fileType = FileType.DescriptorFile;
			}
			else
			{
				ulong extractedNumber;
				var toParse = Regex.Replace(fileName, @"[^\d]", string.Empty);

				if (!ulong.TryParse(toParse, out extractedNumber))
				{
					return false;
				}

				switch (Path.GetExtension(file))
				{
					case Constants.Files.Extensions.LogFile:
						fileType = FileType.LogFile;
						break;
					case Constants.Files.Extensions.TableFile:
						fileType = FileType.TableFile;
						break;
					case Constants.Files.Extensions.TempFile:
						fileType = FileType.TempFile;
						break;
					default:
						return false;
				}

				number = extractedNumber;
			}

			return true;
		}

		private string ExtractFileName(string file)
		{
			var index = file.IndexOf(databaseName, StringComparison.InvariantCultureIgnoreCase);
			if (index == -1)
				return file;

			file = file.Substring(index + databaseName.Length);
			index = file.LastIndexOf("_", StringComparison.InvariantCultureIgnoreCase);
			if (index == -1)
				return file;

			return file.Substring(index + 1);
		}

		public string DescriptorFileName(ulong manifestFileNumber)
		{
			if (manifestFileNumber <= 0)
				throw new InvalidOperationException("ManifestFileNumber");

			return string.Format("{0}{1}", Constants.Files.ManifestPrefix, manifestFileNumber);
		}

		public virtual void RenameFile(string source, string destination)
		{
			var src = Path.Combine(databaseName, source);
			var dst = Path.Combine(databaseName, destination);

			if (!File.Exists(src))
			{
				throw new FileNotFoundException(source);
			}

			if (File.Exists(dst))
			{
				File.Delete(dst);
			}

			File.Move(src, dst);
		}

		public string GetCurrentFileName()
		{
			return Constants.Files.CurrentFile;
		}

		public void EnsureDatabaseDirectoryExists()
		{
			if (Directory.Exists(databaseName))
				return;

			CreateDirectory(databaseName);
		}

		public void CreateDirectory(string name)
		{
			Directory.CreateDirectory(name);
		}

		public virtual IEnumerable<string> GetFiles()
		{
			return new DirectoryInfo(databaseName).GetFiles().Select(x => x.Name);
		}

		public virtual void Lock()
		{
			if (lockFile != null)
				return;
			lockFile =
				new FileStream(Path.Combine(databaseName, Constants.Files.DBLockFile), FileMode.Create, FileAccess.ReadWrite,
							   FileShare.None, 4096, FileOptions.DeleteOnClose);
			TrackResourceUsage.Track(() => lockFile.SafeFileHandle);
		}

		public virtual bool Exists(string name)
		{
			return File.Exists(Path.Combine(databaseName, name));
		}

		public void Dispose()
		{
			if (lockFile != null)
				lockFile.Dispose();
		}

		public string GetTempFileName(ulong fileNumber)
		{
			return GetFileName(Guid.NewGuid().ToString(), fileNumber, Constants.Files.Extensions.TempFile);
		}

		public string GetTableFileName(ulong fileNumber)
		{
			return GetFileName(string.Empty, fileNumber, Constants.Files.Extensions.TableFile);
		}

		public string GetLogFileName(ulong fileNumber)
		{
			return GetFileName(string.Empty, fileNumber, Constants.Files.Extensions.LogFile);
		}

		public virtual Stream OpenForReading(string name)
		{
			var stream = File.Open(Path.Combine(databaseName, name), FileMode.Open, FileAccess.Read);
			TrackResourceUsage.Track(() => stream.SafeFileHandle);
			return stream;
		}

		public virtual IAccessor OpenMemoryMap(string name)
		{
			var file = MemoryMappedFile.CreateFromFile(name, FileMode.Open);
			TrackResourceUsage.Track(() => file.SafeMemoryMappedFileHandle);
			return new MemoryMappedFileAccessor(name ,file);
		}
	}
}