namespace Raven.Storage.Reading
{
	using System.Diagnostics;
	using System.IO;

	using Raven.Storage.Comparing;
	using Raven.Storage.Data;

	public class MergingIterator : IIterator
	{
		private readonly IComparator comparator;

		private IIterator[] children;

		private IIterator current;

		private readonly int n;

		private Direction direction;

		public MergingIterator(IComparator comparator, IIterator[] children, int n)
		{
			this.comparator = comparator;
			this.children = children;
			this.n = n;
			this.direction = Direction.Forward;
			this.current = null;

			for (int i = 0; i < n; i++)
			{
				this.children[i] = children[i];
			}
		}

		internal enum Direction
		{
			Forward = 1,
			Reverse = 2
		}

		public void Dispose()
		{
			this.children = null;
		}

		public bool IsValid
		{
			get
			{
				return this.current != null;
			}
		}

		public void SeekToFirst()
		{
			for (int i = 0; i < n; i++)
			{
				children[i].SeekToFirst();
			}

			FindSmallest();
			direction = Direction.Forward;
		}

		public void SeekToLast()
		{
			for (int i = 0; i < n; i++)
			{
				children[i].SeekToLast();
			}

			FindLargest();
			direction = Direction.Reverse;
		}

		public void Seek(Slice target)
		{
			for (int i = 0; i < n; i++)
			{
				children[i].Seek(target);
			}

			FindSmallest();
			direction = Direction.Forward;
		}

		public void Next()
		{
			Debug.Assert(IsValid);

			// Ensure that all children are positioned after key().
			// If we are moving in the forward direction, it is already
			// true for all of the non-current_ children since current_ is
			// the smallest child and key() == current_->key().  Otherwise,
			// we explicitly position the non-current_ children.
			if (direction != Direction.Forward)
			{
				for (int i = 0; i < n; i++)
				{
					var child = children[i];
					if (child != current)
					{
						child.Seek(Key);
						if (child.IsValid && comparator.Compare(Key, child.Key) == 0)
						{
							child.Next();
						}
					}
				}

				direction = Direction.Forward;
			}

			current.Next();
			FindSmallest();
		}

		public void Prev()
		{
			Debug.Assert(IsValid);

			// Ensure that all children are positioned before key().
			// If we are moving in the reverse direction, it is already
			// true for all of the non-current_ children since current_ is
			// the largest child and key() == current_->key().  Otherwise,
			// we explicitly position the non-current_ children.
			if (direction != Direction.Reverse)
			{
				for (int i = 0; i < n; i++)
				{
					var child = children[i];
					if (child != current)
					{
						child.Seek(Key);
						if (child.IsValid)
						{
							// Child is at first entry >= key().  Step back one to be < key()
							child.Prev();
						}
						else
						{
							// Child has no entries >= key().  Position at last entry.
							child.SeekToLast();
						}
					}
				}

				direction = Direction.Reverse;
			}

			current.Prev();
			FindLargest();
		}

		public Slice Key
		{
			get
			{
				Debug.Assert(IsValid);

				return current.Key;
			}
		}

		public Stream CreateValueStream()
		{
			Debug.Assert(IsValid);

			return current.CreateValueStream();
		}

		private void FindSmallest()
		{
			IIterator smallest = null;
			for (int i = 0; i < n; i++)
			{
				var child = children[i];
				if (child.IsValid)
				{
					if (smallest == null)
					{
						smallest = child;
					}
					else if (comparator.Compare(child.Key, smallest.Key) < 0)
					{
						smallest = child;
					}
				}
			}

			current = smallest;
		}

		private void FindLargest()
		{
			IIterator largest = null;
			for (int i = 0; i < n; i++)
			{
				var child = children[i];
				if (child.IsValid)
				{
					if (largest == null)
					{
						largest = child;
					}
					else if (comparator.Compare(child.Key, largest.Key) > 0)
					{
						largest = child;
					}
				}
			}

			current = largest;
		}
	}
}