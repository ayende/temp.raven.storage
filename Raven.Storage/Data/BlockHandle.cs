using System;
using System.Diagnostics;
using System.IO;
using System.IO.MemoryMappedFiles;
using Raven.Storage.Memory;
using Raven.Storage.Util;

namespace Raven.Storage.Data
{
	[DebuggerDisplay("Pos: {Position} Count: {Count}")]
    public class BlockHandle
    {
	    protected bool Equals(BlockHandle other)
	    {
		    return Position == other.Position && Count == other.Count;
	    }

	    public override bool Equals(object obj)
	    {
		    if (ReferenceEquals(null, obj)) return false;
		    if (ReferenceEquals(this, obj)) return true;
		    if (obj.GetType() != this.GetType()) return false;
		    return Equals((BlockHandle) obj);
	    }

	    public override int GetHashCode()
	    {
		    unchecked
		    {
			    return (Position.GetHashCode()*397) ^ Count.GetHashCode();
		    }
	    }

	    public const int MaxEncodedLength = 10 + 10;

        public long Position { get; set; }
        public long Count { get; set; }

	    public string CacheKey
	    {
		    get { return "BlockHandle: " + Position + "/" + Count; }
	    }

	    public Stream AsStream()
        {
            var ms = new MemoryStream();
            EncodeTo(ms);
            ms.Position = 0;
            return ms;
        }

        public int EncodeTo(Stream stream)
        {
            var size = stream.Write7BitEncodedLong(Position);
            size += stream.Write7BitEncodedLong(Count);
            return size;
        }

	    public int DecodeFrom(IArrayAccessor accessor, int pos)
	    {
		    Position = accessor.Read7BitEncodedLong(ref pos);
		    Count = accessor.Read7BitEncodedLong(ref pos);
		    return pos;
	    }

		public void DecodeFrom(Stream stream)
		{
			Position = stream.Read7BitEncodedLong();
			Count = stream.Read7BitEncodedLong();
		}


    }
}