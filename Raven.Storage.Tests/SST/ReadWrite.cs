using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Runtime.CompilerServices;
using System.Text;
using Raven.Storage.Building;
using Raven.Storage.Data;
using Raven.Storage.Filtering;
using Raven.Storage.Impl;
using Raven.Storage.Memory;
using Raven.Storage.Reading;
using Xunit;

namespace Raven.Storage.Tests.SST
{
	public class ReadWrite : IDisposable
	{
		readonly List<FileStream> shouldHaveBeenDisposed = new List<FileStream>();

		[Fact]
		public void CanReadValuesBackWithoutFilter()
		{
			var state = new StorageState("none", new StorageOptions
			{
				ParanoidChecks = true,
				FilterPolicy = null
			});
			string name;
			using (var file = CreateFile())
			{
				name = file.Name;
				using (var tblBuilder = new TableBuilder(state.Options, file, TempStreamGenerator))
				{
					for (int i = 0; i < 10; i++)
					{
						string k = "tests/" + i.ToString("0000");
						tblBuilder.Add(k, new MemoryStream(Encoding.UTF8.GetBytes(k)));
					}

					tblBuilder.Finish();
					file.Flush(true);
				}
			}

			using (var mmf = MemoryMappedFile.CreateFromFile(name, FileMode.Open))
			{
				var length = new FileInfo(name).Length;
				using (var table = new Table(state, new FileData(new MemoryMappedFileAccessor(mmf), length)))
				using (var iterator = table.CreateIterator(new ReadOptions()))
				{
					for (int i = 0; i < 10; i++)
					{
						string k = "tests/" + i.ToString("0000");
						iterator.Seek(k);
						Assert.True(iterator.IsValid);
						using (var stream = iterator.CreateValueStream())
						using (var reader = new StreamReader(stream))
						{
							Assert.Equal(k, reader.ReadToEnd());
						}
					}
				}
			}
		}

		[Fact]
		public void CanReadValuesBack()
		{
			var state = new StorageState("none", new StorageOptions
				{
					ParanoidChecks = true,
					FilterPolicy = new BloomFilterPolicy()
				});
			const int count = 5;
			string name;
			using (var file = CreateFile())
			{
				name = file.Name;
				using (var tblBuilder = new TableBuilder(state.Options, file, TempStreamGenerator))
				{
					for (int i = 0; i < count; i++)
					{
						string k = "tests/" + i.ToString("0000");
						tblBuilder.Add(k, new MemoryStream(Encoding.UTF8.GetBytes("values/" + i)));
					}

					tblBuilder.Finish();
					file.Flush(true);
				}
			}

			using (var mmf = MemoryMappedFile.CreateFromFile(name, FileMode.Open))
			{
				var length = new FileInfo(name).Length;
				using (var table = new Table(state, new FileData(new MemoryMappedFileAccessor(mmf), length)))
				using (var iterator = table.CreateIterator(new ReadOptions()))
				{
					for (int i = 0; i < count; i++)
					{
						string k = "tests/" + i.ToString("0000");
						iterator.Seek(k);
						Assert.True(iterator.IsValid);
						using (var stream = iterator.CreateValueStream())
						using (var reader = new StreamReader(stream))
						{
							Assert.Equal("values/" + i, reader.ReadToEnd());
						}
					}
				}
			}
		}

		[MethodImpl(MethodImplOptions.NoInlining)]
		private FileStream CreateFile()
		{
			var stackTrace = new StackTrace();
			var f = File.Create(stackTrace.GetFrame(1).GetMethod().Name + ".rsst");

			shouldHaveBeenDisposed.Add(f);

			return f;
		}


		private Stream TempStreamGenerator()
		{
			var s = new FileStream(Path.GetTempFileName(), FileMode.Create, FileAccess.ReadWrite, FileShare.ReadWrite, 4096,
			                      FileOptions.SequentialScan | FileOptions.DeleteOnClose);

			shouldHaveBeenDisposed.Add(s);

			return s;
		}

		public void Dispose()
		{
			foreach (var stream in shouldHaveBeenDisposed)
			{
				Assert.False(stream.CanRead);
			}
		}
	}
}