using System;
using System.IO;
using Raven.Storage.Util;

namespace Raven.Storage.Data
{
    public class Footer
    {
        private static byte[] _tableMagicBytes = BitConverter.GetBytes(TableMagicNumber);

        /// <summary>
        /// kTableMagicNumber was picked by running
        ///    echo http://code.google.com/p/leveldb/ | sha1sum
        /// and taking the leading 64 bits.
        /// </summary>
        public const ulong TableMagicNumber = 0xdb4775248b80fb57ul;

        /// <summary>
        /// Encoded length of a Footer.  Note that the serialization of a
        /// Footer will always occupy exactly this many bytes.  It consists
        /// of two block handles and a magic number.
        /// </summary>
        public const int EncodedLength = 2 * BlockHandle.MaxEncodedLength + 8;

        public BlockHandle MetaIndexHandle { get; set; }
        public BlockHandle IndexHandle { get; set; }

        public void EncodeTo(Stream stream)
        {
            var size = MetaIndexHandle.EncodeTo(stream);
            size += IndexHandle.EncodeTo(stream);

            var buf = new byte[2*BlockHandle.MaxEncodedLength - size];
            stream.Write(buf, 0, buf.Length); // padding
            stream.Write(_tableMagicBytes, 0, _tableMagicBytes.Length);
        }
    }
}