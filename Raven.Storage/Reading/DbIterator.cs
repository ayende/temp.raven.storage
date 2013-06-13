namespace Raven.Storage.Reading
{
	using System.Diagnostics;
	using System.IO;

	using Raven.Storage.Data;
	using Raven.Storage.Impl;

	public class DbIterator : IIterator
	{
		private readonly IIterator iterator;

		private Direction direction;

		private readonly ulong sequence;

		private readonly IStorageContext storageContext;

		private Slice savedKey;

		private Stream savedValueStream;

		public DbIterator(IStorageContext storageContext, IIterator iterator, ulong sequence)
		{
			this.iterator = iterator;
			this.sequence = sequence;
			this.storageContext = storageContext;

			direction = Direction.Forward;

			IsValid = false;
		}

		public void Dispose()
		{
			if (iterator != null)
				iterator.Dispose();
		}

		public bool IsValid { get; private set; }

		public void SeekToFirst()
		{
			direction = Direction.Forward;
			savedValueStream = null;

			iterator.SeekToFirst();

			if (iterator.IsValid)
			{
				FindNextUserEntry(false);
				return;
			}

			IsValid = false;
		}

		public void SeekToLast()
		{
			direction = Direction.Reverse;
			savedValueStream = null;
			iterator.SeekToLast();
			FindPrevUserEntry();
		}

		public void Seek(Slice target)
		{
			direction = Direction.Forward;
			savedKey = null;
			savedValueStream = null;

			savedKey = new InternalKey(target, sequence, ItemType.ValueForSeek).UserKey;
			iterator.Seek(savedKey);

			if (iterator.IsValid)
			{
				FindNextUserEntry(false);
				return;
			}

			IsValid = false;
		}

		public void Next()
		{
			Debug.Assert(IsValid);

			if (direction == Direction.Reverse)
			{
				direction = Direction.Forward;

				// iter_ is pointing just before the entries for this->key(),
				// so advance into the range of entries for this->key() and then
				// use the normal skipping code below.
				if (!iterator.IsValid)
				{
					iterator.SeekToFirst();
				}
				else
				{
					iterator.Next();
				}

				if (!iterator.IsValid)
				{
					IsValid = false;
					savedKey = null;
					return;
				}
			}

			savedKey = InternalKey.ExtractUserKey(iterator.Key);
			FindNextUserEntry(true);
		}

		private void FindNextUserEntry(bool skipping)
		{
			// Loop until we hit an acceptable entry to yield
			Debug.Assert(iterator.IsValid);
			Debug.Assert(direction == Direction.Forward);

			do
			{
				InternalKey internalKey;
				if (InternalKey.TryParse(iterator.Key, out internalKey) && internalKey.Sequence <= sequence)
				{
					switch (internalKey.Type)
					{
						case ItemType.Deletion:
							// Arrange to skip all upcoming entries for this key since
							// they are hidden by this deletion.
							savedKey = internalKey.UserKey;
							skipping = true;
							break;
						case ItemType.Value:
							if (skipping && storageContext.InternalKeyComparator.UserComparator.Compare(internalKey.UserKey, savedKey) <= 0)
							{
								// Entry hidden
							}
							else
							{
								IsValid = true;
								savedKey = null;
								return;
							}
							break;
					}
				}

				iterator.Next();
			}
			while (iterator.IsValid);

			savedKey = null;
			IsValid = false;
		}

		public void Prev()
		{
			Debug.Assert(IsValid);

			if (direction == Direction.Forward)
			{
				// iter_ is pointing at the current entry.  Scan backwards until
				// the key changes so we can use the normal reverse scanning code.
				Debug.Assert(iterator.IsValid); // Otherwise valid_ would have been false
				savedKey = InternalKey.ExtractUserKey(iterator.Key);

				while (true)
				{
					iterator.Prev();
					if (!iterator.IsValid)
					{
						IsValid = false;
						savedKey = null;
						savedValueStream = null;
						return;
					}

					if (storageContext.InternalKeyComparator.UserComparator.Compare(InternalKey.ExtractUserKey(iterator.Key), savedKey)
						< 0)
						break;
				}

				direction = Direction.Reverse;
			}

			FindPrevUserEntry();
		}

		private void FindPrevUserEntry()
		{
			Debug.Assert(direction == Direction.Reverse);

			var itemType = ItemType.Deletion;
			while (iterator.IsValid)
			{
				InternalKey internalKey;
				if (InternalKey.TryParse(this.iterator.Key, out internalKey) && internalKey.Sequence <= this.sequence)
				{
					if ((itemType != ItemType.Deletion)
					    && this.storageContext.InternalKeyComparator.UserComparator.Compare(internalKey.UserKey, this.savedKey) < 0)
					{
						// We encountered a non-deleted value in entries for previous keys,
						break;
					}

					itemType = internalKey.Type;
					if (itemType == ItemType.Deletion)
					{
						this.savedKey = null;
						this.savedValueStream = null;
					}
					else
					{
						this.savedKey = InternalKey.ExtractUserKey(this.iterator.Key);
						this.savedValueStream = this.iterator.CreateValueStream();
					}
				}

				this.iterator.Prev();
			}

			if (itemType == ItemType.Deletion)
			{
				// End
				IsValid = false;
				savedKey = null;
				savedValueStream = null;
				direction = Direction.Forward;
				return;
			}

			IsValid = true;
		}

		public Slice Key
		{
			get
			{
				Debug.Assert(IsValid);
				return direction == Direction.Forward ? InternalKey.ExtractUserKey(iterator.Key) : savedKey;
			}
		}

		public Stream CreateValueStream()
		{
			Debug.Assert(IsValid);
			return direction == Direction.Forward ? iterator.CreateValueStream() : savedValueStream;
		}
	}
}