namespace Raven.Storage.Data
{
	using System;
	using System.Diagnostics;

	using Raven.Storage.Util;

	[DebuggerDisplay("Val: {DebugVal}")]
	public struct InternalKey
	{
		private readonly Slice _userKey;
		private readonly Slice _ikey;
		private readonly ulong _sequence;
		private readonly ItemType _type;

		public Slice UserKey
		{
			get { return _userKey; }
		}

		public ulong Sequence
		{
			get { return _sequence; }
		}

		public ItemType Type
		{
			get { return _type; }
		}

		public Slice TheInternalKey
		{
			get { return _ikey; }
		}

		public InternalKey(Slice userKey, ulong seq, ItemType type)
			: this()
		{
			_userKey = userKey;
			_sequence= seq;
			_type = type;
			_ikey = Encode();
		}

		public InternalKey(Slice key)
			: this()
		{
			_ikey = key;

			if (key.Count < 8)
				throw new ArgumentException("Key is too small");

			var number = BitConverter.ToUInt64(key.Array, key.Offset + key.Count - 8);
			_sequence = number >> 8;
			_type = (ItemType)number;

			if (_type > ItemType.Value)
				throw new ArgumentException("Invalid key type");

			_userKey = new Slice(key.Array, key.Offset, key.Count - 8);
		}

		private Slice Encode()
		{
			var buffer = new byte[UserKey.Count + 8];
			Buffer.BlockCopy(UserKey.Array, UserKey.Offset, buffer, 0, UserKey.Count);
			buffer.WriteLong(UserKey.Count, Format.PackSequenceAndType(Sequence, Type));

			return new Slice(buffer);
		}

		public string DebugVal
		{
			get
			{
				return string.Format("UserKey: {0}, Seq: {1}, Type: {2}", UserKey.DebugVal, Sequence, Type);
			}
		}

		public static bool TryParse(Slice input, out InternalKey internalKey)
		{
			internalKey = new InternalKey();

			if (input.Count < 8)
				return false;

			var number = BitConverter.ToUInt64(input.Array, input.Offset + input.Count - 8);
			var sequence = number >> 8;
			var type = (ItemType)number;

			var key = new Slice(input.Array, input.Offset, input.Count - 8);
			internalKey = new InternalKey(key, sequence, type);

			return type <= ItemType.Value;
		}

	    public override string ToString()
	    {
	        return DebugVal;
	    }

	    public bool Equals(InternalKey other)
	    {
	        return UserKey.Equals(other.UserKey) && Sequence == other.Sequence && Type == other.Type;
	    }

	    public override bool Equals(object obj)
	    {
	        if (ReferenceEquals(null, obj)) return false;
	        return obj is InternalKey && Equals((InternalKey) obj);
	    }

	    public override int GetHashCode()
	    {
	        unchecked
	        {
	            var hashCode = UserKey.GetHashCode();
	            hashCode = (hashCode*397) ^ Sequence.GetHashCode();
	            hashCode = (hashCode*397) ^ (int) Type;
	            return hashCode;
	        }
	    }

		public static Slice ExtractUserKey(Slice a)
		{
			if (a.Array.Length == 0)
				return a;
			if (a.Count < 8)
				throw new ArgumentException("Value '" + a + "' is not an internal key");
			return new Slice(a.Array, a.Offset, a.Count - 8);
		}
	}
}