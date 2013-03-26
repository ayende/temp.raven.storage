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

		}
	}
}
