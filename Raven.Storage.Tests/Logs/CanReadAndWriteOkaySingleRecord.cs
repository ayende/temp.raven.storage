using System;
using System.IO;
using System.Threading.Tasks;
using Raven.Storage.Impl;
using Raven.Storage.Impl.Streams;
using Raven.Storage.Util;
using Xunit;

namespace Raven.Storage.Tests.Logs
{
	public class CanReadAndWriteOkaySingleRecord
	{
		[Fact]
		public async Task Small()
		{
			var buffer = new byte[1044];
			new Random().NextBytes(buffer);

			var memoryStream = new MemoryStream();
			var logWriterStream = new LogWriter(new InMemoryFileSystem("test"), memoryStream, new BufferPool());
			await WriteRecordAsync(logWriterStream, buffer);
			memoryStream.Position = 0;

			var logReader = new LogReader(memoryStream, true, 0, new BufferPool());
			Stream stream;
			Assert.True(logReader.TryReadRecord(out stream));

			var actual = new MemoryStream();
			stream.CopyTo(actual);

			Assert.Equal(buffer, actual.ToArray());

		}

		public static async Task WriteRecordAsync(LogWriter logWriterStream, byte[] buffer)
		{
			logWriterStream.RecordStarted();
			await logWriterStream.WriteAsync(buffer, 0, buffer.Length);
			logWriterStream.RecordCompleted(true);
		}

		[Fact]
		public async Task Big()
		{
			var buffer = new byte[1046 * 32];
			new Random().NextBytes(buffer);

			var memoryStream = new MemoryStream();
			var logWriterStream = new LogWriter(new InMemoryFileSystem("test"), memoryStream, new BufferPool());
			await WriteRecordAsync(logWriterStream, buffer);
		
			memoryStream.Position = 0;

			var logReader = new LogReader(memoryStream, true, 0, new BufferPool());
			Stream stream;
			Assert.True(logReader.TryReadRecord(out stream));

			var actual = new MemoryStream();
			stream.CopyTo(actual);

			Assert.Equal(buffer, actual.ToArray());

		}


		[Fact]
		public async Task VeryBig()
		{
			var buffer = new byte[1046 * 32 * 3];
			new Random().NextBytes(buffer);

			var memoryStream = new MemoryStream();
			var logWriterStream = new LogWriter(new InMemoryFileSystem("test"), memoryStream, new BufferPool());
			await WriteRecordAsync(logWriterStream, buffer);

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