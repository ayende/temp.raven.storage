namespace Raven.Storage.Benchmark.Env
{
	using System;
	using System.Collections.Generic;
	using System.Linq;
	using System.Management;

	public enum CacheLevel : ushort
	{
		Level1 = 3,
		Level2 = 4,
		Level3 = 5,
	}

	public static class CPUInfo
	{
		public static List<uint> GetCacheSizes(CacheLevel level)
		{
			var mc = new ManagementClass("Win32_CacheMemory");
			var moc = mc.GetInstances();
			var cacheSizes = new List<uint>(moc.Count);

			cacheSizes.AddRange(moc
			  .Cast<ManagementObject>()
			  .Where(p => (ushort)(p.Properties["Level"].Value) == (ushort)level)
			  .Select(p => (uint)(p.Properties["MaxCacheSize"].Value)));

			return cacheSizes;
		}

		public static int GetNumberOfProcessors()
		{
			return Environment.ProcessorCount;
		}

		public static long GetCacheSize(CacheLevel level)
		{
			return GetCacheSizes(level).Sum(x => x);
		}
	}
}