// -----------------------------------------------------------------------
//  <copyright file="VersionEditTests.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.IO;
using Raven.Storage.Impl;
using Raven.Storage.Impl.Streams;
using Raven.Storage.Util;
using Xunit;

namespace Raven.Storage.Tests.Compactions
{
	using Raven.Storage.Data;

	public class VersionEditTests
	{
		[Fact]
		public void ShouldEncodeAndDecode()
		{
			var versionEdit = new VersionEdit();

			versionEdit.SetComparatorName("testComparator");
			versionEdit.SetLastSequence(10);
			versionEdit.SetLogNumber(2);
			versionEdit.SetNextFile(3);
			versionEdit.SetPrevLogNumber(1);
			versionEdit.SetLastSequence(1);

			versionEdit.AddFile(0, new FileMetadata()
				{
					FileNumber = 0,
					FileSize = 128,
					LargestKey = new InternalKey("begin", Format.MaxSequenceNumber, ItemType.Value),
					SmallestKey = new InternalKey("end", Format.MaxSequenceNumber, ItemType.Value),
				});
			versionEdit.AddFile(1, new FileMetadata()
			{
				FileNumber = 1,
				FileSize = 128,
				LargestKey = new InternalKey("begin", Format.MaxSequenceNumber, ItemType.Value),
				SmallestKey = new InternalKey("end", Format.MaxSequenceNumber, ItemType.Value),
			});

			versionEdit.DeleteFile(0, 3);
			versionEdit.DeleteFile(1, 4);

			versionEdit.SetCompactionPointer(0, new InternalKey("begin", Format.MaxSequenceNumber, ItemType.Value));
			versionEdit.SetCompactionPointer(1, new InternalKey("end", Format.MaxSequenceNumber, ItemType.Value));

			var memoryStream = new MemoryStream();
			var logWriter = new LogWriter(memoryStream, new BufferPool());
			versionEdit.EncodeTo(logWriter);

			memoryStream.Position = 0;


			var afterDecode = VersionEdit.DecodeFrom(new LogRecordStream(memoryStream, true, new BufferPool()));

			Assert.Equal(versionEdit.Comparator, afterDecode.Comparator);
			Assert.Equal(versionEdit.LogNumber, afterDecode.LogNumber);
			Assert.Equal(versionEdit.NextFileNumber, afterDecode.NextFileNumber);
			Assert.Equal(versionEdit.PrevLogNumber, afterDecode.PrevLogNumber);
			Assert.Equal(versionEdit.LogNumber, afterDecode.LogNumber);
			Assert.Equal(versionEdit.PrevLogNumber, afterDecode.PrevLogNumber);
			Assert.Equal(versionEdit.LastSequence, afterDecode.LastSequence);

			Assert.Equal(versionEdit.NewFiles[0][0].FileNumber, afterDecode.NewFiles[0][0].FileNumber);
			Assert.Equal(versionEdit.NewFiles[0][0].FileSize, afterDecode.NewFiles[0][0].FileSize);
			Assert.Equal(versionEdit.NewFiles[0][0].LargestKey, afterDecode.NewFiles[0][0].LargestKey);
			Assert.Equal(versionEdit.NewFiles[0][0].SmallestKey, afterDecode.NewFiles[0][0].SmallestKey);

			Assert.Equal(versionEdit.NewFiles[1][0].FileNumber, afterDecode.NewFiles[1][0].FileNumber);
			Assert.Equal(versionEdit.NewFiles[1][0].FileSize, afterDecode.NewFiles[1][0].FileSize);
			Assert.Equal(versionEdit.NewFiles[1][0].LargestKey, afterDecode.NewFiles[1][0].LargestKey);
			Assert.Equal(versionEdit.NewFiles[1][0].SmallestKey, afterDecode.NewFiles[1][0].SmallestKey);

			Assert.Equal(versionEdit.DeletedFiles[0][0], afterDecode.DeletedFiles[0][0]);
			Assert.Equal(versionEdit.DeletedFiles[1][0], afterDecode.DeletedFiles[1][0]);

			Assert.Equal(versionEdit.CompactionPointers[0][0], afterDecode.CompactionPointers[0][0]);
			Assert.Equal(versionEdit.CompactionPointers[1][0], afterDecode.CompactionPointers[1][0]);
		} 
	}
}