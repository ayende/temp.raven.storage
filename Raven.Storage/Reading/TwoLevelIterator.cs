namespace Raven.Storage.Reading
{
	using System;
	using System.IO;

	using Raven.Storage.Data;
	using Raven.Storage.Util;

	public class TwoLevelIterator : IIterator
	{
		private readonly IIterator _indexIterator;
		private IIterator _dataIterator;
		private readonly ReadOptions _readOptions;
		private Stream _currentDataHandle;

		private readonly Func<ReadOptions, Stream, IIterator> getIterator;

		public TwoLevelIterator(
			IIterator indexIterator,
			Func<ReadOptions, Stream, IIterator> getIterator,
			ReadOptions readOptions
		)
		{
			_indexIterator = indexIterator;
			_readOptions = readOptions;
			this.getIterator = getIterator;
		}

		public void Dispose()
		{
			_indexIterator.Dispose();
			if (_dataIterator != null)
				_dataIterator.Dispose();
		}

		public void SeekToFirst()
		{
			_indexIterator.SeekToFirst();
			InitDataIterator();
			if (_dataIterator != null)
				_dataIterator.SeekToFirst();
			SkipEmptyDataBlocksForward();
		}

		public void SeekToLast()
		{
			_indexIterator.SeekToLast();
			InitDataIterator();
			if (_dataIterator != null)
				_dataIterator.SeekToLast();
			SkipEmptyDataBlocksBackward();
		}

		public void Seek(Slice target)
		{
			_indexIterator.Seek(target);
			InitDataIterator();
			if(_dataIterator != null) 
				_dataIterator.Seek(target);
			SkipEmptyDataBlocksForward();
		}

		public void Next()
		{
			if (_dataIterator == null)
				throw new InvalidOperationException("Cannot call Next when iterator isn't valid");
			_dataIterator.Next();
			SkipEmptyDataBlocksForward();
		}

		public void Prev()
		{

			if (_dataIterator == null)
				throw new InvalidOperationException("Cannot call Prev when iterator isn't valid");
			_dataIterator.Prev();
			SkipEmptyDataBlocksBackward();
		}

		public Slice Key
		{
			get
			{
				if (_dataIterator == null)
					throw new InvalidOperationException("Cannot call Key when iterator isn't valid");
				return _dataIterator.Key;
			}
		}
		public Stream CreateValueStream()
		{
			if (_dataIterator == null)
				throw new InvalidOperationException("Cannot call CreateValueStream when iterator isn't valid");
			return _dataIterator.CreateValueStream();
		}

		private void SkipEmptyDataBlocksForward()
		{
			while (_dataIterator == null || _dataIterator.IsValid == false)
			{
				if (_indexIterator.IsValid == false)
				{
					SetDataIterator(null);
					return;
				}
				_indexIterator.Next();
				InitDataIterator();
				if (_dataIterator != null)
					_dataIterator.SeekToFirst();
			}
		}

		private void SkipEmptyDataBlocksBackward()
		{
			while (_dataIterator == null || _dataIterator.IsValid == false)
			{
				if (_indexIterator.IsValid == false)
				{
					SetDataIterator(null);
					return;
				}
				_indexIterator.Prev();
				InitDataIterator();
				if (_dataIterator != null)
					_dataIterator.SeekToLast();
			}
		}

		private void InitDataIterator()
		{
			if (_indexIterator.IsValid == false)
			{
				SetDataIterator(null);
				return;
			}

			var handle = _indexIterator.CreateValueStream();

			if (handle.AreEqual(_currentDataHandle)) // nothing to change
				return;

			IIterator blockIterator = null;
			try
			{
				blockIterator = getIterator(_readOptions, handle);
				SetDataIterator(blockIterator);
			}
			catch (Exception)
			{
				if (blockIterator != null)
					blockIterator.Dispose();
				throw;
			}
			_currentDataHandle = handle;
		}

		private void SetDataIterator(IIterator iterator)
		{
			if (_dataIterator != null)
				_dataIterator.Dispose();
			_dataIterator = iterator;
		}

		public bool IsValid { get { return _dataIterator != null; } }
	}
}