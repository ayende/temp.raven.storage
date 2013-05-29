using System;
using System.Collections.Generic;
using System.IO;
using Raven.Storage.Impl.Streams;
using Raven.Storage.Util;
using Xunit;

namespace Raven.Storage.Tests.Logs
{
	public class EdgeCases
	{
	
		[Fact]
		public void BigEnoughForOneFullBlock()
		{
			var random = new Random();
			var buffers = new List<byte[]>();

			for (int i = 0; i < 15; i++)
			{
				var buffer = new byte[LogWriter.BlockSize - LogWriter.HeaderSize];
				random.NextBytes(buffer);
				buffers.Add(buffer);
			}

			var memoryStream = new MemoryStream();
			var logWriterStream = new LogWriter(memoryStream, new BufferPool());

			foreach (var buffer in buffers)
			{
				CanReadAndWriteOkaySingleRecord.WriteRecord(logWriterStream, buffer);
			}

			logWriterStream.Flush();
			memoryStream.Position = 0;

			var logReader = new LogReader(memoryStream, true, 0, new BufferPool());
			for (int i = 0; i < 15; i++)
			{
				Stream stream;
				Assert.True(logReader.TryReadRecord(out stream));

				var actual = new MemoryStream();
				stream.CopyTo(actual);

				Assert.Equal(buffers[i], actual.ToArray());
			}

		}

		[Fact]
		public void SmallLots()
		{
			var random = new Random();
			var buffers = new List<byte[]>();

			const int repeats = 150;
			for (int i = 0; i < repeats; i++)
			{
				var buffer = new byte[LogWriter.BlockSize / 10 - LogWriter.HeaderSize*10];
				random.NextBytes(buffer);
				buffers.Add(buffer);
			}

			var memoryStream = new MemoryStream();
			var logWriterStream = new LogWriter(memoryStream, new BufferPool());

			foreach (var buffer in buffers)
			{
				CanReadAndWriteOkaySingleRecord.WriteRecord(logWriterStream, buffer);
			}

			logWriterStream.Flush();
			memoryStream.Position = 0;

			var logReader = new LogReader(memoryStream, true, 0, new BufferPool());
			for (int i = 0; i < 15; i++)
			{
				Stream stream;
				Assert.True(logReader.TryReadRecord(out stream));

				var actual = new MemoryStream();
				stream.CopyTo(actual);

				Assert.Equal(buffers[i], actual.ToArray());
			}

		}
 
	}
}