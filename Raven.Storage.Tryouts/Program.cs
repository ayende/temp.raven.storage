namespace Raven.Storage.Tryouts
{
	using System.IO;
	using System.Xml;

	using NLog.Config;

	public class Program
	{
		private  static void ConfigureLogging()
		{
			using (var stream = typeof(Program).Assembly.GetManifestResourceStream("Raven.Storage.Tests.NLog.config"))
			using (var reader = XmlReader.Create(stream))
			{
				NLog.LogManager.Configuration = new XmlLoggingConfiguration(reader, "default-config");
			}
		}

		public static void Main(string[] args)
		{
			ConfigureLogging();

			var storage = new Storage("test", new StorageOptions());
			var writeBatch = new WriteBatch();
			writeBatch.Put("test", new MemoryStream());
			storage.Writer.Write(writeBatch);
		}
	}
}
