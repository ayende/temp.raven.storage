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
			var options = new StorageOptions();
			using (var file = File.Create("test2.sst"))
			using (var temp = new FileStream("test.temp", FileMode.Create, FileAccess.ReadWrite,
								FileShare.ReadWrite, 4096, FileOptions.SequentialScan))
			{
				var tblBuilder = new TableBuilder(options, file, temp);

				for (int i = 0; i < 1; i++)
				{
					tblBuilder.Add(("tests/" + i.ToString("0000")), new MemoryStream(Encoding.UTF8.GetBytes("tests/" + i.ToString("0000"))));
				}

				tblBuilder.Finish();
				file.Flush(true);
			}

			using (var mmf = MemoryMappedFile.CreateFromFile("test2.sst", FileMode.Open))
			{
				var length = new FileInfo("test2.sst").Length;
				var table = new Table(options, new FileData(mmf, length));
				using (var iterator = table.CreateIterator(new ReadOptions()))
				{
					iterator.Seek("tests/0000");
					Console.WriteLine(iterator.IsValid);
				}
			}
		}
	}
}
