using System.Diagnostics;
using System.Text;
using Raven.Imports.Newtonsoft.Json;
using System.Linq;

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
			get
			{
				var end = Offset;
				for (; end < Count; end++)
				{
					if (Array[end] < 0x20 || Array[end] > 127)
						break;
				}
				var s = Encoding.UTF8.GetString(Array, Offset, end);
				return s;
			}
		}

		public static implicit operator Slice(string val)
		{
			return new Slice(Encoding.UTF8.GetBytes(val));
		}
	}
}