using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using Raven.Storage.Data;
using Raven.Storage.Memtable;
using Raven.Storage.Util;

namespace Raven.Storage.Filtering
{
	public class FilterBlockBuilder
	{
		const byte FilterBaseLg = 11;
		// Generate new filter every 2KB of data
		const int FilterBase = 1 << FilterBaseLg;

		private readonly List<int> _filterOffsets = new List<int>(); 
		private readonly Stream _stream;
		private readonly IFilterBuilder _filterBuilder;
		private readonly BufferPool _bufferPool = new BufferPool();

		readonly List<Slice> _slices = new List<Slice>();

		public FilterBlockBuilder(Stream stream, IFilterBuilder filterBuilder)
		{
			_stream = stream;
			_filterBuilder = filterBuilder;
		}

		public void Add(Slice key)
		{
			var buffer = _bufferPool.Take(key.Count);
			Buffer.BlockCopy(key.Array, key.Offset, buffer,0, key.Count);
			_slices.Add(new Slice(buffer,0, key.Count));
		}

		public void StartBlock(long pos)
		{
			long filterIndex = pos/FilterBase;
			Debug.Assert(filterIndex >= _filterOffsets.Count);
			while (filterIndex > _filterOffsets.Count)
			{
				GenerateFilter();
			}
		}

		private void GenerateFilter()
		{
			_filterOffsets.Add((int)_stream.Position);

			if (_slices.Count == 0)
			{
				// fast path if ther are not keys for this filter
				return;
			}

			_filterBuilder.CreateFilter(_slices, _stream);
			foreach (var slice in _slices)
			{
				_bufferPool.Return(slice.Array);
			}
			_slices.Clear();
			
		}

		public BlockHandle Finish(Stream output)
		{
			if (_slices.Count > 0)
			{
				GenerateFilter();
			}

			var startPos = output.Position;

			var offsetInBlock = _stream.Position;
			_stream.Position = 0;
			_stream.CopyTo(output);

			foreach (var filterOffset in _filterOffsets)
			{
				output.WriteInt32(filterOffset);
			}
			output.WriteInt32((int)offsetInBlock);
			output.WriteByte(FilterBaseLg);

			return new BlockHandle
				{
					Position = startPos,
					Count = output.Position - startPos
				};
		}

		
	}
}