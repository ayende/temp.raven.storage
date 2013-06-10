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
			var buffer = new byte[key.Count + 8];
			Buffer.BlockCopy(key.Array, key.Offset, buffer, 0, key.Count);
			buffer.WriteLong(key.Count, Format.PackSequenceAndType(seq, type));

			this.UserKey = new Slice(buffer);
			this.Sequence = seq;
			this.Type = type;
		}

		public InternalKey(Slice key)
			: this()
		{
			InternalKey internalKey;
			if (!TryParse(key, out internalKey))
				throw new FormatException("Invalid internal key format.");

			this.Sequence = internalKey.Sequence;
			this.Type = internalKey.Type;
			this.UserKey = internalKey.UserKey;
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
	}
}