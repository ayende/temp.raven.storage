using System;

namespace Raven.Storage.Reading
{
	using System.Collections.Generic;
	using System.Diagnostics;
	using System.IO;

	using Comparing;
	using Data;

	public class MergingIterator : IIterator
	{
		private readonly IComparator comparator;

		private IList<IIterator> children;

		private IIterator current;


		private Direction direction;

		public MergingIterator(IComparator comparator, IList<IIterator> children)
		{
			if (children == null)
				 throw new ArgumentNullException("children");
			
			this.comparator = comparator;
			this.children = children;
			direction = Direction.Forward;
			current = null;
		}

		public void Dispose()
		{
			foreach (var iterator in children)
			{
				iterator.Dispose();
			}
			children = null;
		}

		private void AssertNotDisposed()
		{
			if (children == null)
				throw new ObjectDisposedException("MerginIterator");
		}

		public bool IsValid
		{
			get
			{
				AssertNotDisposed();
				return this.current != null;
			}
		}

		public void SeekToFirst()
		{
			AssertNotDisposed();
			foreach (var iterator in children)
			{
				iterator.SeekToFirst();
			}

			FindSmallest();
			direction = Direction.Forward;
		}

		public void SeekToLast()
		{
			AssertNotDisposed();
			foreach (var iterator in children)
			{
				iterator.SeekToLast();
			}

			FindLargest();
			direction = Direction.Reverse;
		}

		public void Seek(Slice target)
		{
			AssertNotDisposed();
			foreach (var iterator in children)
			{
				iterator.Seek(target);
			}

			FindSmallest();
			direction = Direction.Forward;
		}

		public void Next()
		{
			AssertNotDisposed();
			Debug.Assert(IsValid);

			// Ensure that all children are positioned after key().
			// If we are moving in the forward direction, it is already
			// true for all of the non-current_ children since current_ is
			// the smallest child and key() == current_->key().  Otherwise,
			// we explicitly position the non-current_ children.
			if (direction != Direction.Forward)
			{
				foreach (var child in children)
				{
					if (child == current)
						continue;
					child.Seek(Key);
					if (child.IsValid && comparator.Compare(Key, child.Key) == 0)
					{
						child.Next();
					}
				}

				direction = Direction.Forward;
			}

			current.Next();
			FindSmallest();
		}

		public void Prev()
		{
			AssertNotDisposed();
			Debug.Assert(IsValid);

			// Ensure that all children are positioned before key().
			// If we are moving in the reverse direction, it is already
			// true for all of the non-current_ children since current_ is
			// the largest child and key() == current_->key().  Otherwise,
			// we explicitly position the non-current_ children.
			if (direction != Direction.Reverse)
			{
				foreach (var child in children)
				{
					if (child == current)
						continue;
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

				direction = Direction.Reverse;
			}

			current.Prev();
			FindLargest();
		}

		public Slice Key
		{
			get
			{
				AssertNotDisposed();
				Debug.Assert(IsValid);

				return current.Key;
			}
		}

		public Stream CreateValueStream()
		{
			AssertNotDisposed();
			Debug.Assert(IsValid);

			return current.CreateValueStream();
		}

		private void FindSmallest()
		{
			IIterator smallest = null;
			foreach (var child in children)
			{
				if (!child.IsValid) 
					continue;
				if (smallest == null)
				{
					smallest = child;
				}
				else if (comparator.Compare(child.Key, smallest.Key) < 0)
				{
					smallest = child;
				}
			}

			current = smallest;
		}

		private void FindLargest()
		{
			IIterator largest = null;
			foreach (var child in children)
			{
				if (!child.IsValid) 
					continue;
				if (largest == null)
				{
					largest = child;
				}
				else if (comparator.Compare(child.Key, largest.Key) > 0)
				{
					largest = child;
				}
			}

			current = largest;
		}
	}
}