namespace Raven.Storage.Data
{
	using System;
	using System.Diagnostics;

	using Raven.Storage.Util;

	[DebuggerDisplay("Val: {DebugVal}")]
	public struct InternalKey
	{
		public Slice UserKey { get; private set; }

		public ulong Sequence { get; private set; }

		public ItemType Type { get; private set; }

		public InternalKey(Slice key, ulong seq, ItemType type)
			: this()
		{
			this.UserKey = key;
			this.Sequence = seq;
			this.Type = type;
		}

		public InternalKey(Slice key)
			: this()
		{
			InternalKey internalKey;
			if (!TryParse(key, out internalKey))
				throw new FormatException("Invalid internal key format.");

			Sequence = internalKey.Sequence;
			Type = internalKey.Type;
			UserKey = internalKey.UserKey;
		}

		public Slice Encode()
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
	}
}