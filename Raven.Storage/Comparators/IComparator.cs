using System;

namespace Raven.Storage.Comparators
{
    public interface IComparator
    {
		string Name { get; }
        int Compare(ArraySegment<byte> a, ArraySegment<byte> b);
        int FindSharedPrefix(ArraySegment<byte> a, ArraySegment<byte> b);
        ArraySegment<byte> FindShortestSeparator(ArraySegment<byte> a, ArraySegment<byte> b);
        ArraySegment<byte> FindShortestSuccessor(ArraySegment<byte> key);
    }
}