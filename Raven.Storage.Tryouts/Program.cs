using System.IO;
using System.IO.MemoryMappedFiles;
using System.Text;
using Raven.Storage.Building;
using Raven.Storage.Reading;

namespace Raven.Storage.Tryouts
{
	class Program
	{
		static void Main()
		{
			var options = new StorageOptions();
			using (var file = File.Create("test.sst"))
			using (var temp = new FileStream(Path.GetTempFileName(), FileMode.Create, FileAccess.ReadWrite,
								FileShare.None, 4096, FileOptions.DeleteOnClose | FileOptions.SequentialScan))
			{
				var tblBuilder = new TableBuilder(options, file, temp);

				for (int i = 0; i < 100; i++)
				{
					var key = "tests/" + i.ToString("0000");
					tblBuilder.Add(key, new MemoryStream(Encoding.UTF8.GetBytes(key)));
				}

				tblBuilder.Finish();
			}

			using (var mmf = MemoryMappedFile.CreateFromFile("test.sst", FileMode.Open))
			{
				var table = new Table(options, mmf, new FileInfo("test.sst").Length);
			}
		}
	}
}
