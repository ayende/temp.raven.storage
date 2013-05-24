namespace Raven.Storage.Tests.Compaction
{
	using Raven.Storage.Data;
	using Raven.Storage.Impl;

	using Xunit;

	public class StatusTests
	{
		[Fact]
		public void FirstMessageEncoding()
		{
			var status = Status.NotFound("test");
			Assert.Equal("NotFound:test", status.ToString());
		}

		[Fact]
		public void SecondMessageEncoding()
		{
			var status = Status.NotFound("", "test");
			Assert.Equal("NotFound:: test", status.ToString());
		}

		[Fact]
		public void BothMessagesEncoding()
		{
			var status = Status.NotFound("test", "test2");
			Assert.Equal("NotFound:test: test2", status.ToString());
		}

		[Fact]
		public void BothMessagesNullEncoding()
		{
			var status = Status.NotFound("", "");
			Assert.Equal("NotFound:", status.ToString());
		}

		[Fact]
		public void PredefinedStatuses()
		{
			Assert.True(Status.OK().IsOK());
			Assert.True(Status.NotFound(new Slice()).IsNotFound());
			Assert.True(Status.Corruption(new Slice()).IsCorruption());
			Assert.True(Status.NotSupported(new Slice()).IsNotSupported());
			Assert.True(Status.InvalidArgument(new Slice()).IsInvalidArgument());
			Assert.True(Status.IOError(new Slice()).IsIOError());
		}
	}
}