using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;

namespace Raven.Storage.Impl.Streams
{
	public class AsyncBinaryWriter : IDisposable
	{
		private readonly bool _leaveOpen;
		private Stream stream;
		private Encoding _encoding;
		private byte[] _buffer;
		private bool disposed;
		private int maxCharsPerRound;
		private byte[] stringBuffer;


		public AsyncBinaryWriter(Stream output)
			: this(output, Encoding.UTF8)
		{
		}


		public AsyncBinaryWriter(Stream output, Encoding encoding)
			: this(output, encoding, false)
		{
		}

		public AsyncBinaryWriter(Stream output, Encoding encoding, bool leaveOpen)
		{
			if (output == null)
				throw new ArgumentNullException("output");
			if (encoding == null)
				throw new ArgumentNullException("encoding");
			if (!output.CanWrite)
				throw new ArgumentException("Stream does not support writing or already closed.");

			_leaveOpen = leaveOpen;
			stream = output;
			_encoding = encoding;
			_buffer = new byte[16];
		}


		public void Dispose()
		{
			Dispose(true);
		}

		public void Close()
		{
			Dispose(true);
		}

		protected void Dispose(bool disposing)
		{
			if (disposing && stream != null && !_leaveOpen)
				stream.Close();

			_buffer = null;
			_encoding = null;
			disposed = true;
		}

		public virtual Task FlushAsync()
		{
			return stream.FlushAsync();
		}

		public virtual long Seek(int offset, SeekOrigin origin)
		{
			return stream.Seek(offset, origin);
		}

		public Task WriteAsync(bool value)
		{
			if (disposed)
				throw new ObjectDisposedException("BinaryWriter", "Cannot write to a closed BinaryWriter");

			_buffer[0] = (byte)(value ? 1 : 0);
			return stream.WriteAsync(_buffer, 0, 1);
		}

		public Task WriteAsync(byte value)
		{
			if (disposed)
				throw new ObjectDisposedException("BinaryWriter", "Cannot write to a closed BinaryWriter");

			_buffer[0] = value;
			return stream.WriteAsync(_buffer, 0, 1);
		}

		public Task WriteAsync(byte[] buffer)
		{
			if (disposed)
				throw new ObjectDisposedException("BinaryWriter", "Cannot write to a closed BinaryWriter");

			if (buffer == null)
				throw new ArgumentNullException("buffer");
			return stream.WriteAsync(buffer, 0, buffer.Length);
		}

		public Task WriteAsync(byte[] buffer, int index, int count)
		{
			if (disposed)
				throw new ObjectDisposedException("BinaryWriter", "Cannot write to a closed BinaryWriter");

			if (buffer == null)
				throw new ArgumentNullException("buffer");
			return stream.WriteAsync(buffer, index, count);
		}

		public Task WriteAsync(char ch)
		{
			if (disposed)
				throw new ObjectDisposedException("BinaryWriter", "Cannot write to a closed BinaryWriter");

			var dec = new char[1];
			dec[0] = ch;
			byte[] enc = _encoding.GetBytes(dec, 0, 1);
			return stream.WriteAsync(enc, 0, enc.Length);
		}

		public Task WriteAsync(char[] chars)
		{
			if (disposed)
				throw new ObjectDisposedException("BinaryWriter", "Cannot write to a closed BinaryWriter");

			if (chars == null)
				throw new ArgumentNullException("chars");
			byte[] enc = _encoding.GetBytes(chars, 0, chars.Length);
			return stream.WriteAsync(enc, 0, enc.Length);
		}

		public Task WriteAsync(char[] chars, int index, int count)
		{
			if (disposed)
				throw new ObjectDisposedException("BinaryWriter", "Cannot write to a closed BinaryWriter");

			if (chars == null)
				throw new ArgumentNullException("chars");
			byte[] enc = _encoding.GetBytes(chars, index, count);
			return stream.WriteAsync(enc, 0, enc.Length);
		}

		public virtual unsafe Task Write(decimal value)
		{
			if (disposed)
				throw new ObjectDisposedException("BinaryWriter", "Cannot write to a closed BinaryWriter");

			var valuePtr = (byte*)&value;

			/*
			 * decimal in stream is lo32, mi32, hi32, ss32
			 * but its internal structure si ss32, hi32, lo32, mi32
			 */

			if (BitConverter.IsLittleEndian)
			{
				for (int i = 0; i < 16; i++)
				{
					if (i < 4)
						_buffer[i + 12] = valuePtr[i];
					else if (i < 8)
						_buffer[i + 4] = valuePtr[i];
					else if (i < 12)
						_buffer[i - 8] = valuePtr[i];
					else
						_buffer[i - 8] = valuePtr[i];
				}
			}
			else
			{
				for (int i = 0; i < 16; i++)
				{
					if (i < 4)
						_buffer[15 - i] = valuePtr[i];
					else if (i < 8)
						_buffer[15 - i] = valuePtr[i];
					else if (i < 12)
						_buffer[11 - i] = valuePtr[i];
					else
						_buffer[19 - i] = valuePtr[i];
				}
			}

			return stream.WriteAsync(_buffer, 0, 16);
		}

		public Task WriteAsync(double value)
		{
			if (disposed)
				throw new ObjectDisposedException("BinaryWriter", "Cannot write to a closed BinaryWriter");

			return stream.WriteAsync(BitConverter.GetBytes(value), 0, 8);
		}

		public Task WriteAsync(short value)
		{
			if (disposed)
				throw new ObjectDisposedException("BinaryWriter", "Cannot write to a closed BinaryWriter");

			_buffer[0] = (byte)value;
			_buffer[1] = (byte)(value >> 8);
			return stream.WriteAsync(_buffer, 0, 2);
		}

		public Task WriteAsync(int value)
		{
			if (disposed)
				throw new ObjectDisposedException("BinaryWriter", "Cannot write to a closed BinaryWriter");

			_buffer[0] = (byte)value;
			_buffer[1] = (byte)(value >> 8);
			_buffer[2] = (byte)(value >> 16);
			_buffer[3] = (byte)(value >> 24);
			return stream.WriteAsync(_buffer, 0, 4);
		}


		public Task WriteAsync(long value)
		{
			if (disposed)
				throw new ObjectDisposedException("BinaryWriter", "Cannot write to a closed BinaryWriter");

			for (int i = 0, sh = 0; i < 8; i++, sh += 8)
				_buffer[i] = (byte)(value >> sh);
			return stream.WriteAsync(_buffer, 0, 8);
		}

		[CLSCompliant(false)]
		public Task WriteAsync(sbyte value)
		{
			if (disposed)
				throw new ObjectDisposedException("BinaryWriter", "Cannot write to a closed BinaryWriter");

			_buffer[0] = (byte)value;
			return stream.WriteAsync(_buffer, 0, 1);
		}

		public Task WriteAsync(float value)
		{
			if (disposed)
				throw new ObjectDisposedException("BinaryWriter", "Cannot write to a closed BinaryWriter");

			return stream.WriteAsync(BitConverter.GetBytes(value), 0, 4);
		}

		public async Task Write(string value)
		{
			if (disposed)
				throw new ObjectDisposedException("BinaryWriter", "Cannot write to a closed BinaryWriter");

			int len = _encoding.GetByteCount(value);
			Write7BitEncodedInt(len);

			if (stringBuffer == null)
			{
				stringBuffer = new byte[512];
				maxCharsPerRound = 512 / _encoding.GetMaxByteCount(1);
			}

			int chpos = 0;
			int chrem = value.Length;
			while (chrem > 0)
			{
				int cch = (chrem > maxCharsPerRound) ? maxCharsPerRound : chrem;
				int blen = _encoding.GetBytes(value, chpos, cch, stringBuffer, 0);
				await stream.WriteAsync(stringBuffer, 0, blen);

				chpos += cch;
				chrem -= cch;
			}
		}

		[CLSCompliant(false)]
		public Task WriteAsync(ushort value)
		{
			if (disposed)
				throw new ObjectDisposedException("BinaryWriter", "Cannot write to a closed BinaryWriter");

			_buffer[0] = (byte)value;
			_buffer[1] = (byte)(value >> 8);
			return stream.WriteAsync(_buffer, 0, 2);
		}

		[CLSCompliant(false)]
		public Task WriteAsync(uint value)
		{
			if (disposed)
				throw new ObjectDisposedException("BinaryWriter", "Cannot write to a closed BinaryWriter");

			_buffer[0] = (byte)value;
			_buffer[1] = (byte)(value >> 8);
			_buffer[2] = (byte)(value >> 16);
			_buffer[3] = (byte)(value >> 24);
			return stream.WriteAsync(_buffer, 0, 4);
		}

		[CLSCompliant(false)]
		public Task WriteAsync(ulong value)
		{
			if (disposed)
				throw new ObjectDisposedException("BinaryWriter", "Cannot write to a closed BinaryWriter");

			for (int i = 0, sh = 0; i < 8; i++, sh += 8)
				_buffer[i] = (byte)(value >> sh);
			return stream.WriteAsync(_buffer, 0, 8);
		}

		protected void Write7BitEncodedInt(int value)
		{
			do
			{
				int high = (value >> 7) & 0x01ffffff;
				var b = (byte)(value & 0x7f);

				if (high != 0)
				{
					b = (byte)(b | 0x80);
				}

				Write(b);
				value = high;
			} while (value != 0);
		}
	}
}