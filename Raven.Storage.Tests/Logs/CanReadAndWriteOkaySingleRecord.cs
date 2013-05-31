using System;
using System.IO;
using Raven.Storage.Impl.Streams;
using Raven.Storage.Util;
using Xunit;

namespace Raven.Storage.Tests.Logs
{
	public class CanReadAndWriteOkaySingleRecord
	{
		[Fact]
		public void Small()
		{
			var buffer = new byte[1044];
			new Random().NextBytes(buffer);

			var memoryStream = new MemoryStream();
			var logWriterStream = new LogWriter(memoryStream, new BufferPool());
			WriteRecord(logWriterStream, buffer);
			logWriterStream.Flush();
			memoryStream.Position = 0;

			var logReader = new LogReader(memoryStream, true, 0, new BufferPool());
			Stream stream;
			Assert.True(logReader.TryReadRecord(out stream));

			var actual = new MemoryStream();
			stream.CopyTo(actual);

			Assert.Equal(buffer, actual.ToArray());

		}

		public static void WriteRecord(LogWriter logWriterStream, byte[] buffer)
		{
			logWriterStream.RecordStarted();
			logWriterStream.WriteAsync(buffer, 0, buffer.Length).Wait();
			logWriterStream.RecordCompletedAsync().Wait();
		}

		[Fact]
		public void Big()
		{
			var buffer = new byte[1046 * 32];
			new Random().NextBytes(buffer);

			var memoryStream = new MemoryStream();
			var logWriterStream = new LogWriter(memoryStream, new BufferPool());
			WriteRecord(logWriterStream, buffer);
			logWriterStream.Flush();
		
			memoryStream.Position = 0;

			var logReader = new LogReader(memoryStream, true, 0, new BufferPool());
			Stream stream;
			Assert.True(logReader.TryReadRecord(out stream));

			var actual = new MemoryStream();
			stream.CopyTo(actual);

			Assert.Equal(buffer, actual.ToArray());

		}


		[Fact]
		public void VeryBig()
		{
			var buffer = new byte[1046 * 32 * 3];
			new Random().NextBytes(buffer);

			var memoryStream = new MemoryStream();
			var logWriterStream = new LogWriter(memoryStream, new BufferPool());
			WriteRecord(logWriterStream, buffer);
			logWriterStream.Flush();

			memoryStream.Position = 0;

			var logReader = new LogReader(memoryStream, true, 0, new BufferPool());
			Stream stream;
			Assert.True(logReader.TryReadRecord(out stream));

			var actual = new MemoryStream();
			stream.CopyTo(actual);

			Assert.Equal(buffer, actual.ToArray());

		} 
	}
}