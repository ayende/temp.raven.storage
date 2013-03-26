using System.Diagnostics;
using System.Text;

namespace Raven.Storage.Data
{
	[DebuggerDisplay("Val: {DebugVal}")]
	public struct Slice
	{
		private readonly byte[] _array;
		private readonly int _count;
		private readonly int _offset;

		public Slice(byte[] array) : this(array, 0, array.Length)
		{
			
		}
		
		public Slice(byte[] array, int offset, int count)
		{
			_array = array;
			_count = count;
			_offset = offset;
		}

		public byte[] Array
		{
			get { return _array; }
		}

		public int Count
		{
			get { return _count; }
		}

		public int Offset
		{
			get { return _offset; }
		}

		public string DebugVal
		{
			get { return Encoding.UTF8.GetString(Array, Offset, Count); }
		}

		public static implicit operator Slice(string val)
		{
			return new Slice(Encoding.UTF8.GetBytes(val));
		}
	}
}