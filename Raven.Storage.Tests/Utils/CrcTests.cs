// -----------------------------------------------------------------------
//  <copyright file="CrcTests.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Text;
using Raven.Storage.Util;
using Xunit;

namespace Raven.Storage.Tests.Utils
{
	public class CrcTests
	{
		[Fact]
		public void ExtendShouldConcatenateCorrectlyAndProduceConsistentResult()
		{
			var hello = Encoding.UTF8.GetBytes("hello ");
			var world = Encoding.UTF8.GetBytes("world");
			var helloWorld = Encoding.UTF8.GetBytes("hello world");

			Assert.Equal(Crc.Value(helloWorld, 0, helloWorld.Length), Crc.Extend(Crc.Value(hello, 0, hello.Length), world, 0, world.Length));
		}

		[Fact]
		public void ShouldCorrectlyMaskAndUnmask()
		{
			var foo = Encoding.UTF8.GetBytes("foo");
			var crc = Crc.Value(foo, 0, foo.Length);
			Assert.NotEqual((int)crc, Crc.Mask(crc));
			Assert.NotEqual((int)crc, Crc.Mask((uint)Crc.Mask(crc)));
			Assert.Equal(crc, Crc.Unmask(Crc.Mask(crc)));
			Assert.Equal(crc, Crc.Unmask((int)Crc.Unmask(Crc.Mask((uint)Crc.Mask(crc)))));
		}
	}
}