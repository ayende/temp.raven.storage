using System.IO;
using Raven.Storage.Util;

namespace Raven.Storage.Data
{
    public class BlockHandle
    {
        public const int MaxEncodedLength = 10 + 10;

        public long Position { get; set; }
        public long Count { get; set; }

        public Stream AsStream()
        {
            var ms = new MemoryStream();
            EncodeTo(ms);
            ms.Position = 0;
            return ms;
        }

        public int EncodeTo(Stream stream)
        {
            var size = stream.Write7BitEncodedLong(Position);
            size += stream.Write7BitEncodedLong(Count);
            return size;
        }
    }
}