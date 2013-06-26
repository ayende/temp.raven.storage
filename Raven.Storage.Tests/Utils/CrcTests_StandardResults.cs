﻿// -----------------------------------------------------------------------
//  <copyright file="CrcTests.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using Raven.Storage.Util;
using Xunit;

namespace Raven.Storage.Tests.Utils
{
	// From rfc3720 section B.4.
	public class CrcTests_StandardResults
	{
		[Fact]
		public void Zeros()
		{
			var data = new byte[32];

			var crc = Crc.Value(data, 0, 32);

			Assert.Equal(0x8a9136aau, crc);
		}

		[Fact]
		public void MaxByteValues()
		{
			var data = new byte[32];

			for (int i = 0; i < 32; i++)
			{
				data[i] = 0xff;
			}

			var crc = Crc.Value(data, 0, 32);

			Assert.Equal(0x62a8ab43u, crc);
		}

		[Fact]
		public void SequentialNumbers()
		{
			var data = new byte[32];

			for (byte i = 0; i < 32; i++)
			{
				data[i] = i;
			}

			var crc = Crc.Value(data, 0, 32);

			Assert.Equal(0x46dd794eu, crc);
		}

		[Fact]
		public void SequentialNumbers_Reversed()
		{
			var data = new byte[32];

			for (byte i = 0; i < 32; i++)
			{
				data[i] = (byte) (31 - i);
			}

			var crc = Crc.Value(data, 0, 32);

			Assert.Equal(0x113fdb5cu, crc);
		}

		[Fact]
		public void DefinedData()
		{
			var data = new byte[48]
				{
					0x01, 0xc0, 0x00, 0x00,
					0x00, 0x00, 0x00, 0x00,
					0x00, 0x00, 0x00, 0x00,
					0x00, 0x00, 0x00, 0x00,
					0x14, 0x00, 0x00, 0x00,
					0x00, 0x00, 0x04, 0x00,
					0x00, 0x00, 0x00, 0x14,
					0x00, 0x00, 0x00, 0x18,
					0x28, 0x00, 0x00, 0x00,
					0x00, 0x00, 0x00, 0x00,
					0x02, 0x00, 0x00, 0x00,
					0x00, 0x00, 0x00, 0x00,
				};

			var crc = Crc.Value(data, 0, 48);

			Assert.Equal(0xd9963a56, crc);
		}
	}
}