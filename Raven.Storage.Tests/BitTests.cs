namespace Raven.Storage.Tests
{
	using System;

	using Raven.Storage.Util;

	using Xunit;

	public class BitTests
	{
		[Fact]
		public void T1()
		{
			var buffer1 = new byte[4];
			var buffer2 = new byte[8];

			var random = new Random();

			for (int i = 0; i < 10000; i++)
			{
				var value = random.Next();

				Bit.Set(buffer1, 0, value);
				Assert.Equal(BitConverter.GetBytes(value), buffer1);

				Bit.Set(buffer1, 0, (uint)value);
				Assert.Equal(BitConverter.GetBytes((uint)value), buffer1);

				Bit.Set(buffer2, 0, (long)value);
				Assert.Equal(BitConverter.GetBytes((long)value), buffer2);

				Bit.Set(buffer2, 0, (ulong)value);
				Assert.Equal(BitConverter.GetBytes((ulong)value), buffer2);
			}
		}
	}
}