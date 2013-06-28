namespace Raven.Storage.Util
{
	public static class Bit
	{
		public unsafe static void Set(byte[] buffer, int index, int value)
		{
			fixed (byte* p = &buffer[index])
			{
				*((int*)p) = value;
			}
		}

		public unsafe static void Set(byte[] buffer, int index, uint value)
		{
			fixed (byte* p = &buffer[index])
			{
				*((uint*)p) = value;
			}
		}

		public unsafe static void Set(byte[] buffer, int index, long value)
		{
			fixed (byte* p = &buffer[index])
			{
				*((long*)p) = value;
			}
		}

		public unsafe static void Set(byte[] buffer, int index, ulong value)
		{
			fixed (byte* p = &buffer[index])
			{
				*((ulong*)p) = value;
			}
		}
	}
}