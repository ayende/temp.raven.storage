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
			get { return Encoding.UTF8.GetString(Array, Offset, Count); }
		}

		public static implicit operator Slice(string val)
		{
			return new Slice(Encoding.UTF8.GetBytes(val));
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

		public void RemovePrefix(int prefixLength)
		{
			if (prefixLength > Count)
				throw new InvalidOperationException(string.Format("Prefix length: {0}. Array Count: {1}", prefixLength, Count));

			_offset += prefixLength;
			_count -= prefixLength;
		}

		public bool IsEmpty()
		{
			return _count <= 0;
		}
	}
}