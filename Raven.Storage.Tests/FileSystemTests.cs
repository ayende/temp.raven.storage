// -----------------------------------------------------------------------
//  <copyright file="FileSystemTests.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Raven.Storage.Impl;
using Xunit;

namespace Raven.Storage.Tests
{
	public class FileSystemTests : IDisposable
	{
		private readonly FileSystem fileSystem;
		private const string DatabaseName = "test";

		public FileSystemTests()
		{
			if(Directory.Exists(DatabaseName))
				Directory.Delete(DatabaseName, true);

			fileSystem = new FileSystem(DatabaseName);
			fileSystem.EnsureDatabaseDirectoryExists();
		}

		[Fact]
		public void ShouldCreateFilesInDatabaseDirectory()
		{
			using (fileSystem.NewWritable("file"))
			{
				Assert.True(File.Exists(Path.Combine(DatabaseName, "file")));
			}	
		}

		[Fact]
		public void FileNameShouldNotContainDatabaseDirectory()
		{
			var name = fileSystem.GetFileName("file", 1, ".txt");
			Assert.DoesNotContain(DatabaseName, name);
		}

		[Fact]
		public void FullFileNameShouldContainDatabaseDirectory()
		{
			var name = fileSystem.GetFullFileName("file", 1, ".txt");
			Assert.Equal(Path.Combine(DatabaseName, "file000001.txt"), name);
		}

		[Fact]
		public void ShouldDeleteFileFromDatabaseDirectory()
		{
			using (fileSystem.NewWritable("file")) { }

			Assert.True(File.Exists(Path.Combine(DatabaseName, "file")));

			fileSystem.DeleteFile("file");

			Assert.False(File.Exists(Path.Combine(DatabaseName, "file")));
		}

		[Fact]
		public void ShouldRenameFileInDatabaseDirectory()
		{
			using (fileSystem.NewWritable("file")) { }

			Assert.True(File.Exists(Path.Combine(DatabaseName, "file")));

			fileSystem.RenameFile("file", "renamed");

			Assert.False(File.Exists(Path.Combine(DatabaseName, "file")));
			Assert.True(File.Exists(Path.Combine(DatabaseName, "renamed")));
		}

		[Fact]
		public void ShouldGetAllFiles()
		{
			using (fileSystem.NewWritable("file1")) { }
			using (fileSystem.NewWritable("file2")) { }

			Assert.Equal(2, fileSystem.GetFiles().Count());
		}

		[Fact]
		public void ShouldCorrectlyParseDatabaseFiles()
		{
			using (fileSystem.NewWritable(Constants.Files.CurrentFile)) { }
			using (fileSystem.NewWritable(Constants.Files.DBLockFile)) { }
			using (fileSystem.NewWritable(Constants.Files.LogFile)) { }
			using (fileSystem.NewWritable(fileSystem.DescriptorFileName(1))) { }
			using (fileSystem.NewWritable(fileSystem.GetFileName("log", 1, Constants.Files.Extensions.LogFile))) { }
			using (fileSystem.NewWritable(fileSystem.GetFileName("temp", 1, Constants.Files.Extensions.TempFile))) { }
			using (fileSystem.NewWritable(fileSystem.GetFileName("table", 1, Constants.Files.Extensions.TableFile))) { }

			var types = new List<FileType>();

			foreach (var file in fileSystem.GetFiles())
			{
				ulong number;
				FileType type;
				Assert.True(fileSystem.TryParseDatabaseFile(file, out number, out type));

				types.Add(type);
			}

			Assert.Contains(FileType.LogFile, types);
			Assert.Contains(FileType.DBLockFile, types);
			Assert.Contains(FileType.TableFile, types);
			Assert.Contains(FileType.DescriptorFile, types);
			Assert.Contains(FileType.CurrentFile, types);
			Assert.Contains(FileType.TempFile, types);
			Assert.Contains(FileType.InfoLogFile, types);
		}

		public void Dispose()
		{
			Directory.Delete(DatabaseName, true);
		}
	}
}