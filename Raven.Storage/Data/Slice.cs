namespace Raven.Storage.Data
{
	using System;
	using System.Diagnostics;
	using System.Text;

	using Raven.Storage.Comparing;

	[DebuggerDisplay("Val: {DebugVal}")]
	public struct Slice
	{
		private readonly byte[] _array;
		private int _count;
		private int _offset;

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
			return new Slice(!string.IsNullOrEmpty(val) ? Encoding.UTF8.GetBytes(val) : new byte[0]);
		}

		public int CompareTo(Slice other, IComparator comparator = null)
		{
			if (comparator == null)
			{
				comparator = ByteWiseComparator.Default;
			}

			return comparator.Compare(this, other);
		}

		public bool StartsWith(Slice other)
		{
			var comparator = ByteWiseComparator.Default;
			var otherSize = other.Count;

			return comparator.FindSharedPrefix(this, other) == otherSize;
		}


		public bool IsEmpty()
		{
			return _count <= 0;
		}

		public override string ToString()
		{
		    return DebugVal;
		}

		public override bool Equals(object obj)
		{
		    if (obj is Slice == false)
		        return false;
		    var other = (Slice) obj;

		    if (other._count != _count)
		        return false;

		    for (int i = 0; i < _count; i++)
		    {
		        if (_array[i + _offset] != other._array[i + other._offset])
		            return false;
		    }
		    return true;
		}

		public override int GetHashCode()
		{
            var hashCode = 0;
            for (int i = 0; i < _count; i++)
		    {
		        hashCode = (hashCode*397) ^ _array[i + _offset];
		    }
            return hashCode;
		} 
	}
}