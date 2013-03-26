using System;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Text;
using Raven.Storage.Building;
using Raven.Storage.Data;
using Raven.Storage.Reading;
using Raven.Storage.Util;

namespace Raven.Storage.Tryouts
{
	class Program
	{
		static void Main()
		{
			var options = new StorageOptions
				{
					ParanoidChecks = true
				};
			using (var file = File.Create("test2.sst"))
			{
				using (var tblBuilder = new TableBuilder(options, file, TempStreamGenerator))
				{
					for (int i = 0; i < 1; i++)
					{
						string k = "tests/" + i.ToString("0000");
						tblBuilder.Add(k, new MemoryStream(Encoding.UTF8.GetBytes(k)));
					}

					tblBuilder.Finish();
					file.Flush(true);
				}
			}

			using (var mmf = MemoryMappedFile.CreateFromFile("test2.sst", FileMode.Open))
			{
				var length = new FileInfo("test2.sst").Length;
				var table = new Table(options, new FileData(mmf, length));
				using (var iterator = table.CreateIterator(new ReadOptions()))
				{
					iterator.Seek("teSTS/0005");
					Console.WriteLine(iterator.IsValid);
					using(var stream = iterator.CreateValueStream())
					using(var reader = new StreamReader(stream))
					{
						Console.WriteLine(reader.ReadToEnd());
					}
				}
			}
		}

		private static Stream TempStreamGenerator()
		{
			return new FileStream(Path.GetTempFileName(), FileMode.Create, FileAccess.ReadWrite, FileShare.ReadWrite, 4096,
			                      FileOptions.SequentialScan | FileOptions.DeleteOnClose);
		}
	}
}
