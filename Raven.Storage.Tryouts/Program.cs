using System;
using System.Collections.Generic;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Text;
using Raven.Storage.Building;
using Raven.Storage.Data;
using Raven.Storage.Filtering;
using Raven.Storage.Reading;
using Raven.Storage.Tests.Filtering;

namespace Raven.Storage.Tryouts
{
	class Program
	{
	public static uint RotateRight( uint value, int count)
	{
		return (value >> count) | (value << (32 - count));
	}
		static void Main()
		{
			//uint x = 254262435u;

			//Console.WriteLine(x);
			//Console.WriteLine(RotateRight(x, 17));
			//uint a = (x >> 17);
			//uint b = (x << 15);
			//Console.WriteLine(a + " " + b+ " " + (a | b));
			
			//// OUTPUT:
			//// 254262435
			//// 1939 3729883136 3729885075

			//var bloomFilterBuilder = new BloomFilterBuilder(10, 6, new BloomFilterPolicy(caseInsensitive: false));
			//bloomFilterBuilder.CreateFilter(new List<Slice>{"test"}, new MemoryStream() );

			new BloomFilterTest().CanProperlyMatch();

			//var options = new StorageOptions
			//	{
			//		ParanoidChecks = true
			//	};
			//using (var file = File.Create("test2.sst"))
			//{
			//	using (var tblBuilder = new TableBuilder(options, file, TempStreamGenerator))
			//	{
			//		for (int i = 0; i < 1; i++)
			//		{
			//			string k = "tests/" + i.ToString("0000");
			//			tblBuilder.Add(k, new MemoryStream(Encoding.UTF8.GetBytes(k)));
			//		}

			//		tblBuilder.Finish();
			//		file.Flush(true);
			//	}
			//}

			//using (var mmf = MemoryMappedFile.CreateFromFile("test2.sst", FileMode.Open))
			//{
			//	var length = new FileInfo("test2.sst").Length;
			//	var table = new Table(options, new FileData(mmf, length));
			//	using (var iterator = table.CreateIterator(new ReadOptions()))
			//	{
			//		iterator.Seek("teSTS/0005");
			//		Console.WriteLine(iterator.IsValid);
			//		using(var stream = iterator.CreateValueStream())
			//		using(var reader = new StreamReader(stream))
			//		{
			//			Console.WriteLine(reader.ReadToEnd());
			//		}
			//	}
			//}
		}

		private static Stream TempStreamGenerator()
		{
			return new FileStream(Path.GetTempFileName(), FileMode.Create, FileAccess.ReadWrite, FileShare.ReadWrite, 4096,
			                      FileOptions.SequentialScan | FileOptions.DeleteOnClose);
		}
	}
}
