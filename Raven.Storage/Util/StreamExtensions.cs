using System.IO;

namespace Raven.Storage.Util
{
    public static class StreamExtensions
    {
        public static int Write7BitEncodedInt(this Stream stream, int value)
        {
            int size = 0;
            var num = (uint)value;
            while (num >= 128U)
            {
                size++;
                stream.WriteByte((byte)(num | 128U));
                num >>= 7;
            }
            stream.WriteByte((byte)num);
            return size + 1;
        }

        public static int Write7BitEncodedLong(this Stream stream, long value)
        {
            int size = 0;
            var num = (ulong)value;
            while (num >= 128U)
            {
                size++;
                stream.WriteByte((byte)(num | 128U));
                num >>= 7;
            }
            stream.WriteByte((byte)num);
            return size + 1;
        }

        public static void Write32BitInt(this Stream stream, int value)
        {
            var buffer = new byte[4]
                {
                    (byte) value,
                    (byte) (value >> 8),
                    (byte) (value >> 16),
                    (byte) (value >> 24)
                };
            stream.Write(buffer, 0, 4);
        } 
    }
}