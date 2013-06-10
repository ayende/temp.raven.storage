namespace Raven.Storage.Data
{
	using System;

	using Raven.Storage.Util;

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

		public Slice Encode()
		{
			var buffer = new byte[UserKey.Count + 8];
			Buffer.BlockCopy(UserKey.Array, UserKey.Offset, buffer, 0, UserKey.Count);
			buffer.WriteLong(UserKey.Count, Format.PackSequenceAndType(Sequence, Type));

			return new Slice(buffer);
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