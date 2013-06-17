using Raven.Storage.Data;

namespace Raven.Storage.Comparing
{
    public interface IComparator
    {
		string Name { get; }

		/// <summary>
		/// Compare the two values
		/// </summary>
        int Compare(Slice a, Slice b);

		/// <summary>
		/// Advanced: Used to reduce the space requirements of intenral data structures. 
		/// Clients are urged to default to the implementation in ByteWiseComparator.
		///
		/// Find the shared prefix of those two keys.
		/// </summary>
        int FindSharedPrefix(Slice a, Slice b);

		/// <summary>
		/// Advanced: Used to reduce the space requirements of intenral data structures. 
		/// Clients are urged to default to the implementation in ByteWiseComparator.
		/// 
		/// If start is smaller than the limit, try to return a short value that is bigger than start and smaller then limit.
		/// </summary>
        void FindShortestSeparator(ref Slice start, Slice limit);


		/// <summary>
		/// Advanced: Used to reduce the space requirements of intenral data structures. 
		/// Clients are urged to default to the implementation in ByteWiseComparator.
		/// 
		/// Return a value that is shorter than key and larger than it.
		/// Will try to use the scratch buffer as a way to avoid memory allocations
		/// </summary>
		Slice FindShortestSuccessor(Slice key, ref byte[] scratch);
    }
}