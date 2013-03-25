using System.IO;

namespace Raven.Storage.Util
{
    public class CrcStream : Stream
    {
        /// <summary>
        /// Encapsulate a <see cref="System.IO.Stream" />.
        /// </summary>
        /// <param name="stream">The stream to calculate the checksum for.</param>
        public CrcStream(Stream stream)
        {
            this.stream = stream;
        }

	    readonly Stream stream;

        /// <summary>
        /// Gets the underlying stream.
        /// </summary>
        public Stream Stream
        {
            get { return stream; }
        }

        public override bool CanRead
        {
            get { return stream.CanRead; }
        }

        public override bool CanSeek
        {
            get { return stream.CanSeek; }
        }

        public override bool CanWrite
        {
            get { return stream.CanWrite; }
        }

        public override void Flush()
        {
            stream.Flush();
        }

        public override System.Threading.Tasks.Task FlushAsync(System.Threading.CancellationToken cancellationToken)
        {
            return stream.FlushAsync(cancellationToken);
        }

        public override long Length
        {
            get { return stream.Length; }
        }

        public override long Position
        {
            get
            {
                return stream.Position;
            }
            set
            {
                stream.Position = value;
            }
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            return stream.Seek(offset, origin);
        }

        public override void SetLength(long value)
        {
            stream.SetLength(value);
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            count = stream.Read(buffer, offset, count);
            readCrc = CalculateCrc(readCrc, buffer, offset, count);
            return count;
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            stream.Write(buffer, offset, count);

            writeCrc = CalculateCrc(writeCrc, buffer, offset, count);
        }

        uint CalculateCrc(uint crc, byte[] buffer, int offset, int count)
        {
            unchecked
            {
                for (int i = offset, end = offset + count; i < end; i++)
                    crc = (crc >> 8) ^ table[(crc ^ buffer[i]) & 0xFF];
            }
            return crc;
        }

        static private readonly uint[] table = GenerateTable();

        static private uint[] GenerateTable()
        {
            unchecked
            {
                uint[] table = new uint[256];

                uint crc;
                const uint poly = 0xEDB88320;
                for (uint i = 0; i < table.Length; i++)
                {
                    crc = i;
                    for (int j = 8; j > 0; j--)
                    {
                        if ((crc & 1) == 1)
                            crc = (crc >> 1) ^ poly;
                        else
                            crc >>= 1;
                    }
                    table[i] = crc;
                }

                return table;
            }

        }

        uint readCrc = unchecked(0xFFFFFFFF);

        /// <summary>
        /// Gets the CRC checksum of the data that was read by the stream thus far.
        /// </summary>
        public uint ReadCrc
        {
            get { return unchecked(readCrc ^ 0xFFFFFFFF); }
        }

        uint writeCrc = unchecked(0xFFFFFFFF);

        /// <summary>
        /// Gets the CRC checksum of the data that was written to the stream thus far.
        /// </summary>
        public uint WriteCrc
        {
            get { return unchecked(writeCrc ^ 0xFFFFFFFF); }
        }

        /// <summary>
        /// Resets the read and write checksums.
        /// </summary>
        public void ResetChecksum()
        {
            readCrc = unchecked(0xFFFFFFFF);
            writeCrc = unchecked(0xFFFFFFFF);
        }

		public override System.Threading.Tasks.Task CopyToAsync(Stream destination, int bufferSize, System.Threading.CancellationToken cancellationToken)
		{
			return stream.CopyToAsync(destination, bufferSize, cancellationToken);
		}
    }
}