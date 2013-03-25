using System.IO;
using System.Text;
using Raven.Storage.Building;

namespace Raven.Storage.Tryouts
{
    class Program
    {
        static void Main(string[] args)
        {
	        var options = new StorageOptions();
			using (var file = File.Create("test.sst"))
			{
				var tblBuilder = new TableBuilder(options, file);

				for (int i = 0; i < 100; i++)
				{
					var key = "tests/" + i.ToString("0000");
					tblBuilder.Add(key, new MemoryStream(Encoding.UTF8.GetBytes(key)));
				}

				tblBuilder.Finish();
			}
        }
    }
}
