using System;
using System.Collections.Generic;
using System.IO;
using System.Text.RegularExpressions;

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

		public virtual Stream NewWritable(string name)
		{
			return File.OpenWrite(Path.Combine(databaseName, name));
		}

		public virtual Stream NewReadableWritable(string name)
		{
			return File.Open(Path.Combine(databaseName, name), FileMode.CreateNew, FileAccess.ReadWrite);
		}

		public Stream NewWritable(string name, ulong num, string ext)
		{
			return NewWritable(GetFileName(name, num, ext));
		}

		public void DeleteFile(string name)
		{
			if (File.Exists(Path.Combine(databaseName, name)))
				File.Delete(Path.Combine(databaseName, name));
		}

		public bool TryParseDatabaseFile(FileSystemInfo file, out ulong number, out FileType fileType)
		{
			number = 0;
			fileType = FileType.Unknown;

			if (file.Name.Equals(Constants.Files.CurrentFile, StringComparison.InvariantCultureIgnoreCase))
			{
				number = 0;
				fileType = FileType.CurrentFile;
			}
			else if (file.Name.Equals(Constants.Files.DBLockFile, StringComparison.InvariantCultureIgnoreCase))
			{
				number = 0;
				fileType = FileType.DBLockFile;
			}
			else if (file.Name.Equals(Constants.Files.LogFile) || file.FullName.Equals(Constants.Files.CurrentFile + ".old"))
			{
				number = 0;
				fileType = FileType.InfoLogFile;
			}
			else if (file.Name.StartsWith(Constants.Files.ManifestPrefix, StringComparison.InvariantCultureIgnoreCase))
			{
				if (!string.IsNullOrEmpty(file.Extension))
				{
					return false;
				}

				var prefixLength = Constants.Files.ManifestPrefix.Length;
				if (!ulong.TryParse(file.Name.Substring(prefixLength, file.Name.Length - prefixLength), out number))
				{
					return false;
				}

				fileType = FileType.DescriptorFile;
			}
			else
			{
				ulong extractedNumber;
				var toParse = Regex.Replace(file.Name, @"[^\d]", string.Empty);

				if (!ulong.TryParse(toParse, out extractedNumber))
				{
					return false;
				}

				switch (file.Extension)
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

		public string DescriptorFileName(ulong manifestFileNumber)
		{
			if (manifestFileNumber <= 0)
				throw new InvalidOperationException("ManifestFileNumber");

			return string.Format("{0}{1}", Constants.Files.ManifestPrefix, manifestFileNumber);
		}

		public void RenameFile(string source, string destination)
		{
			File.Move(Path.Combine(databaseName, source), Path.Combine(databaseName, destination));
		}

		public string GetCurrentFileName()
		{
			return Constants.Files.CurrentFile;
		}

		public void EnsureDatabaseDirectoryExists()
		{
			if(Directory.Exists(databaseName))
				return;

			CreateDirectory(databaseName);
		}

		public void CreateDirectory(string name)
		{
			Directory.CreateDirectory(name);
		}

		public IEnumerable<FileSystemInfo> GetFiles()
		{
			return new DirectoryInfo(databaseName).GetFiles();
		}

		public void Lock()
		{
			lockFile = File.Open(Path.Combine(databaseName, Constants.Files.DBLockFile), FileMode.CreateNew, FileAccess.ReadWrite, FileShare.None);
		}

		public bool Exists(string name)
		{
			return File.Exists(Path.Combine(databaseName, name));
		}

		public void Dispose()
		{
			lockFile.Dispose();
		}

		public string GetTempFileName(ulong fileNumber)
		{
			return GetFileName(databaseName, fileNumber, Constants.Files.Extensions.TempFile);
		}

		public string GetTableFileName(ulong fileNumber)
		{
			return GetFileName(databaseName, fileNumber, Constants.Files.Extensions.TableFile);
		}
	}
}