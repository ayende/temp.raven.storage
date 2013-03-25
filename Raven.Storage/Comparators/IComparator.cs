using System;

namespace Raven.Storage.Comparators
{
    public interface IComparator
    {
		string Name { get; }
		/// <summary>
		/// Compare the two values
		/// </summary>
        int Compare(ArraySegment<byte> a, ArraySegment<byte> b);

		/// <summary>
		/// Advanced: Used to reduce the space requirements of intenral data structures. 
		/// Clients are urged to default to the implementation in LexicographicalByteWiseComparator.
		///
		/// Find the shared prefix of those two keys.
		/// </summary>
        int FindSharedPrefix(ArraySegment<byte> a, ArraySegment<byte> b);

		/// <summary>
		/// Advanced: Used to reduce the space requirements of intenral data structures. 
		/// Clients are urged to default to the implementation in LexicographicalByteWiseComparator.
		/// 
		/// If start is smaller than the limit, try to return a short value that is bigger than start and smaller then limit.
		/// Will try to use the scratch buffer as a way to avoid memory allocations if needed
		/// </summary>
        ArraySegment<byte> FindShortestSeparator(ArraySegment<byte> start, ArraySegment<byte> limit, byte[] scratch);


		/// <summary>
		/// Advanced: Used to reduce the space requirements of intenral data structures. 
		/// Clients are urged to default to the implementation in LexicographicalByteWiseComparator.
		/// 
		/// Return a value that is shorter than key and larger than it.
		/// Will try to use the scratch buffer as a way to avoid memory allocations
		/// </summary>
		ArraySegment<byte> FindShortestSuccessor(ArraySegment<byte> key, byte[] scratch);
    }
}