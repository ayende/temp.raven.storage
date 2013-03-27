using System;
using Raven.Storage.Util;

namespace Raven.Storage.Tryouts
{
	class Program
	{
		static void Main()
		{
			Console.WriteLine(Info.GetPowerOfTwo(1024*1024*4));
		}
	}
}
