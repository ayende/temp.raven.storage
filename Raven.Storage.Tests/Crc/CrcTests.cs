// -----------------------------------------------------------------------
//  <copyright file="CrcTests.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Text;
using Xunit;

namespace Raven.Storage.Tests.Crc
{
	public class CrcTests
	{
		[Fact]
		public void ExtendShouldConcatenateCorrectlyAndProduceConsistentResult()
		{
			var hello = Encoding.UTF8.GetBytes("hello ");
			var world = Encoding.UTF8.GetBytes("world");
			var helloWorld = Encoding.UTF8.GetBytes("hello world");

			Assert.Equal(Util.Crc.Value(helloWorld, 0, helloWorld.Length), Util.Crc.Extend(Util.Crc.Value(hello, 0, hello.Length), world, 0, world.Length));
		}

		[Fact]
		public void ShouldCorrectlyMaskAndUnmask()
		{
			var foo = Encoding.UTF8.GetBytes("foo");
			var crc = Util.Crc.Value(foo, 0, foo.Length);
			Assert.NotEqual((int)crc, Util.Crc.Mask(crc));
			Assert.NotEqual((int)crc, Util.Crc.Mask((uint)Util.Crc.Mask(crc)));
			Assert.Equal(crc, Util.Crc.Unmask(Util.Crc.Mask(crc)));
			Assert.Equal(crc, Util.Crc.Unmask((int)Util.Crc.Unmask(Util.Crc.Mask((uint)Util.Crc.Mask(crc)))));
		}
	}
}