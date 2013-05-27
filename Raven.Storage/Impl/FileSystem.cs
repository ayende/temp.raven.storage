using System;
using System.IO;

namespace Raven.Storage.Impl
{
	public class FileSystem : IDisposable
	{
		public string GetFileName(string name, ulong num, string ext)
		{
			return string.Format("{0}{1:000000}.{2}", name, num, ext);
		}

		public virtual Stream NewWritable(string name)
		{
			return File.OpenWrite(name);
		}

		public Stream NewWritable(string name, ulong num, string ext)
		{
			return NewWritable(GetFileName(name, num, ext));
		}

		public void DeleteFile(string name)
		{
			if (File.Exists(name))
				File.Delete(name);
		}

		public bool TryParseDatabaseFile(FileSystemInfo file, out ulong number, out FileType fileType)
		{
			number = 0;
			fileType = FileType.Unknown;

			if (file.FullName.Equals(Constants.Files.CurrentFile, StringComparison.InvariantCultureIgnoreCase))
			{
				number = 0;
				fileType = FileType.CurrentFile;
			}
			else if (file.FullName.Equals(Constants.Files.DBLockFile, StringComparison.InvariantCultureIgnoreCase))
			{
				number = 0;
				fileType = FileType.DBLockFile;
			}
			else if (file.FullName.Equals(Constants.Files.LogFile) || file.FullName.Equals(Constants.Files.CurrentFile + ".old"))
			{
				number = 0;
				fileType = FileType.InfoLogFile;
			}
			else if (file.FullName.StartsWith(Constants.Files.ManifestPrefix, StringComparison.InvariantCultureIgnoreCase))
			{
				if (!string.IsNullOrEmpty(file.Extension))
				{
					return false;
				}

				var prefixLength = Constants.Files.ManifestPrefix.Length;
				if (!ulong.TryParse(file.FullName.Substring(prefixLength, file.FullName.Length - prefixLength), out number))
				{
					return false;
				}

				fileType = FileType.DescriptorFile;
			}
			else
			{
				ulong extractedNumber;
				if (!ulong.TryParse(file.Name, out extractedNumber))
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

		public string DescriptorFileName(string databaseName, ulong manifestFileNumber)
		{
			if (manifestFileNumber <= 0)
				throw new InvalidOperationException("ManifestFileNumber");

			return string.Format("{0}/{1}{2}", databaseName, Constants.Files.ManifestPrefix, manifestFileNumber);
		}

		public void RenameFile(string source, string destination)
		{
			File.Move(source, destination);
		}

		public string GetCurrentFileName(string databaseName)
		{
			return string.Format("{0}/{1}", databaseName, Constants.Files.CurrentFile);
		}

		public void Dispose()
		{
		}
	}
}