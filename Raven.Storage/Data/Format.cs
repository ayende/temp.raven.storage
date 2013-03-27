namespace Raven.Storage.Data
{
	public static class Format
	{
		/// <summary>
		/// we leave 8 bits empty at the bottom so a type & a sequence
		/// can be packed to gether into 64 bits
		/// </summary>
		public const ulong MaxSequenceNumber = (0x1ul << 56) - 1;

		public static ulong PackSequenceAndType(ulong seq, ItemType type)
		{
			return (seq << 8) | (byte) type;
		}
	}
}