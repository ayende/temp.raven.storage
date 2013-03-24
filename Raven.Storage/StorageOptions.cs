using System;
using System.IO;
using Raven.Storage.Data;

namespace Raven.Storage
{
    public class StorageOptions
    {
        public ICompareKeys Comparer { get; set; }
        public IFilterPolicy FilterPolicy { get; set; }
        public int BlockSize { get; set; }
        public int BlockRestartInterval { get; set; }
    }

    public interface IFilterPolicy
    {
        IFilterBuilder CreateBuidler();
        string Name { get; }
    }

    public interface IFilterBuilder
    {
        void Add(ArraySegment<byte> key);
        void StartBlock(long pos);
        BlockHandle Finish(Stream stream);
    }
}