using System.IO;
using Raven.Storage.Comparing;
using Raven.Storage.Data;
using Raven.Storage.Impl;
using Raven.Storage.Memtable;
using Xunit;

namespace Raven.Storage.Tests.Memtable
{
    public class CanProperlySort
    {
        [Fact]
        public void ShouldSortHigherSeqeuencesLater()
        {
            using (var memTable = new MemTable(new StorageState("test", new StorageOptions())))
            {
                var memoryHandle = memTable.Write(new MemoryStream());
                memTable.Add(1, ItemType.Value, "1", memoryHandle);

                memoryHandle = memTable.Write(new MemoryStream());
                memTable.Add(2, ItemType.Value, "1", memoryHandle);

                using (var it = memTable.NewIterator())
                {
                    it.SeekToFirst();
                    Assert.True(it.IsValid);
                    Assert.Equal(new InternalKey("1", 1, ItemType.Value).TheInternalKey, it.Key);
                    it.Next();
                    Assert.True(it.IsValid);
                    Assert.Equal(new InternalKey("1", 2, ItemType.Value).TheInternalKey, it.Key);
                }
            }
        }
    }
}