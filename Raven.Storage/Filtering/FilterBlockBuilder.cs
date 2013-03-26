using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using Raven.Storage.Data;
using Raven.Storage.Util;

namespace Raven.Storage.Filtering
{
	public class FilterBlockBuilder
	{
		const byte FilterBaseLg = 11;
		// Generate new filter every 2KB of data
		const int FilterBase = 1 << FilterBaseLg;

		private readonly List<int> _filterOffsets = new List<int>(); 
		private readonly LinkedList<byte[]> _bufferPool = new LinkedList<byte[]>();
		private readonly Stream _stream;
		private readonly IFilterBuilder _filterBuilder;

		readonly List<Slice> _slices = new List<Slice>();

		public FilterBlockBuilder(Stream stream, IFilterBuilder filterBuilder)
		{
			_stream = stream;
			_filterBuilder = filterBuilder;
		}

		public void Add(Slice key)
		{
			var buffer = GetBufferFromPool(key.Count);
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
				_bufferPool.AddLast(slice.Array);
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

			_stream.Position = 0;
			_stream.CopyTo(output);

			foreach (var filterOffset in _filterOffsets)
			{
				output.Write32BitInt(filterOffset);
			}
			output.Write32BitInt(_filterOffsets.Count);
			output.WriteByte(FilterBaseLg);

			return new BlockHandle
				{
					Position = startPos,
					Count = output.Position - startPos
				};
		}

		private byte[] GetBufferFromPool(int size)
		{
			size = GetPowerOfTwo(size);
			var node = _bufferPool.First;
			// first pass, try to find exact match
			while (node != null)
			{
				if (node.Value.Length == size)
				{
					_bufferPool.Remove(node);
					break;
				}
				node = node.Next;
			}
			if (node == null)
			{
				node = _bufferPool.First;
				// second pass, try to find anything that matches

				while (node != null)
				{
					if (node.Value.Length <= size)
					{
						_bufferPool.Remove(node);
						break;
					}
					node = node.Next;
				}
			}

			return node != null ? node.Value : new byte[size];
		}


		private int GetPowerOfTwo(int v)
		{
			// see http://graphics.stanford.edu/~seander/bithacks.html#RoundUpPowerOf2
			v--;
			v |= v >> 1;
			v |= v >> 2;
			v |= v >> 4;
			v |= v >> 8;
			v |= v >> 16;
			v++;
			return v;
		}
	}
}