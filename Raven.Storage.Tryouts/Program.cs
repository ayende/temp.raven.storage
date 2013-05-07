using System.IO;

namespace Raven.Storage.Tryouts
{
	class Program
	{
		static void Main(string[] args)
		{
			var storage = new Storage("test", new StorageOptions());
			var writeBatch = new WriteBatch();
			writeBatch.Put("test", new MemoryStream());
			storage.Writer.Write(writeBatch);
		}
	}
}
