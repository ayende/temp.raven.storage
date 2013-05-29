namespace Raven.Storage.Data
{
	using System;

	public struct ParsedInternalKey
	{
		public Slice UserKey { get; private set; }

		public ulong Sequence { get; private set; }

		public ItemType Type { get; private set; }

		public ParsedInternalKey(Slice key, ulong seq, ItemType type)
			: this()
		{
			this.UserKey = key;
			this.Sequence = seq;
			this.Type = type;
		}

		public static bool TryParseInternalKey(Slice input, out ParsedInternalKey internalKey)
		{
			internalKey = new ParsedInternalKey();

			if (input.Count < 8)
				return false;

			var number = BitConverter.ToUInt64(input.Array, input.Offset + input.Count - 8);
			var sequence = number >> 8;
			var type = (ItemType)number;

			var key = new Slice(input.Array, input.Offset, input.Count - 8);
			internalKey = new ParsedInternalKey(key, sequence, type);

			return type <= ItemType.Value;
		}
	}
}