using System;
using System.Collections.Generic;
using System.IO;
using System.IO.MemoryMappedFiles;
using Raven.Storage.Data;
using Raven.Storage.Filtering;
using Raven.Storage.Memory;
using Xunit;

namespace Raven.Storage.Tests.Filtering
{
	public class BloomFilterTest : IDisposable
	{
		readonly List<IDisposable> disposables = new List<IDisposable>();

		[Fact]
		 public void CanConstructFilter()
		 {
			 var bloomFilterPolicy = new BloomFilterPolicy();
			 var builder = bloomFilterPolicy.CreateBuilder();
			var filterBlockBuilder = new FilterBlockBuilder(new MemoryStream(), builder);
			filterBlockBuilder.StartBlock(0);
			filterBlockBuilder.Add(new InternalKey("test/1", 0, ItemType.Value).TheInternalKey);
			filterBlockBuilder.Add(new InternalKey("test/2", 0, ItemType.Value).TheInternalKey);
			filterBlockBuilder.Add(new InternalKey("test/231", 0, ItemType.Value).TheInternalKey);
			var memoryStream = new MemoryStream();
			filterBlockBuilder.Finish(memoryStream);

			 Assert.NotEqual(0, memoryStream.Length);
		 }

		 [Fact]
		 public void CanReadFilter()
		 {
			 var bloomFilterPolicy = new BloomFilterPolicy();
			 var builder = bloomFilterPolicy.CreateBuilder();
			 var filterBlockBuilder = new FilterBlockBuilder(new MemoryStream(), builder);
			 filterBlockBuilder.StartBlock(0);
			 filterBlockBuilder.Add(new InternalKey("test/1", 0, ItemType.Value).TheInternalKey);
			 filterBlockBuilder.Add(new InternalKey("test/2", 0, ItemType.Value).TheInternalKey);
			 filterBlockBuilder.Add(new InternalKey("test/231", 0, ItemType.Value).TheInternalKey);
			 var memoryStream = new MemoryStream();
			 filterBlockBuilder.Finish(memoryStream);

			 var filter = bloomFilterPolicy.CreateFilter(ToMemoryMappedViewAccessor(memoryStream));
			 Assert.NotNull(filter);
		 }

		 [Fact]
		 public void CanProperlyMatch()
		 {
			 var bloomFilterPolicy = new BloomFilterPolicy(caseInsensitive:false);
			 var builder = bloomFilterPolicy.CreateBuilder();
			 var filterBlockBuilder = new FilterBlockBuilder(new MemoryStream(), builder);
			 filterBlockBuilder.StartBlock(0);
			 filterBlockBuilder.Add(new InternalKey("test", 0, ItemType.Value).TheInternalKey);
			 var memoryStream = new MemoryStream();
			 filterBlockBuilder.Finish(memoryStream);

			 var filter = bloomFilterPolicy.CreateFilter(ToMemoryMappedViewAccessor(memoryStream));
			 Assert.True(filter.KeyMayMatch(0, "test"));
			 Assert.False(filter.KeyMayMatch(0, "tester"));
		 }

		 [Fact]
		 public void CanProperlyMatchCaseInsensitive()
		 {
			 var bloomFilterPolicy = new BloomFilterPolicy();
			 var builder = bloomFilterPolicy.CreateBuilder();
			 var filterBlockBuilder = new FilterBlockBuilder(new MemoryStream(), builder);
			 filterBlockBuilder.StartBlock(0);
			 filterBlockBuilder.Add(new InternalKey("test", 0, ItemType.Value).TheInternalKey);
			 var memoryStream = new MemoryStream();
			 filterBlockBuilder.Finish(memoryStream);

			 var filter = bloomFilterPolicy.CreateFilter(ToMemoryMappedViewAccessor(memoryStream));
			 Assert.True(filter.KeyMayMatch(0, "Test"));
			 Assert.True(filter.KeyMayMatch(0, "TEST"));
		 }

		 [Fact]
		 public void CanProperlyMatchComplex()
		 {
			 var bloomFilterPolicy = new BloomFilterPolicy(caseInsensitive: false);
			 var builder = bloomFilterPolicy.CreateBuilder();
			 var filterBlockBuilder = new FilterBlockBuilder(new MemoryStream(), builder);
			 filterBlockBuilder.StartBlock(0);
			 filterBlockBuilder.Add(new InternalKey("test/1", 0, ItemType.Value).TheInternalKey);
			 filterBlockBuilder.Add(new InternalKey("test/2", 0, ItemType.Value).TheInternalKey);
			 filterBlockBuilder.Add(new InternalKey("test/3", 0, ItemType.Value).TheInternalKey);
			 filterBlockBuilder.Add(new InternalKey("test/142", 0, ItemType.Value).TheInternalKey);
			 filterBlockBuilder.Add(new InternalKey("test/3432", 0, ItemType.Value).TheInternalKey);
			 var memoryStream = new MemoryStream();
			 filterBlockBuilder.Finish(memoryStream);

			 var filter = bloomFilterPolicy.CreateFilter(ToMemoryMappedViewAccessor(memoryStream));
			 Assert.True(filter.KeyMayMatch(0, "test/1"));
			 Assert.True(filter.KeyMayMatch(0, "test/142"));
			 Assert.True(filter.KeyMayMatch(0, "test/3"));
			 Assert.True(filter.KeyMayMatch(0, "test/3432"));
			 Assert.False(filter.KeyMayMatch(0, "tester"));
			 Assert.False(filter.KeyMayMatch(0, "test/5"));
			 Assert.False(filter.KeyMayMatch(0, "test/133"));
			 Assert.False(filter.KeyMayMatch(0, "test/133"));
			 Assert.True(filter.KeyMayMatch(0, "test/144")); // even though this is wrong, false positives are okay with bloom filters
		 }


		private IArrayAccessor ToMemoryMappedViewAccessor(Stream stream)
		{
			var memoryMappedFile = MemoryMappedFile.CreateNew(Guid.NewGuid().ToString(), stream.Length);
			disposables.Add(memoryMappedFile);

			stream.Position = 0;
			using(var mms = memoryMappedFile.CreateViewStream(0, stream.Length))
				stream.CopyTo(mms);

			var accessor = memoryMappedFile.CreateViewAccessor(0, stream.Length, MemoryMappedFileAccess.Read);
			disposables.Add(accessor);
			return new MemoryMappedFileArrayAccessor(accessor);
		}

		public void Dispose()
		{
			foreach (var disposable in disposables)
			{
				disposable.Dispose();
			}
		}
	}
}