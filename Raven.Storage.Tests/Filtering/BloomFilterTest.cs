using System;
using System.Collections.Generic;
using System.IO;
using System.IO.MemoryMappedFiles;
using Raven.Storage.Data;
using Raven.Storage.Filtering;
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
			filterBlockBuilder.Add("test/1");
			filterBlockBuilder.Add("test/2");
			filterBlockBuilder.Add("test/231");
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
			 filterBlockBuilder.Add("test/1");
			 filterBlockBuilder.Add("test/2");
			 filterBlockBuilder.Add("test/231");
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
			 filterBlockBuilder.Add("test");
			 var memoryStream = new MemoryStream();
			 filterBlockBuilder.Finish(memoryStream);

			 var filter = bloomFilterPolicy.CreateFilter(ToMemoryMappedViewAccessor(memoryStream));
			 Assert.True(filter.KeyMayMatch(0, "test/1"));
			 Assert.True(filter.KeyMayMatch(0, "test/2"));
			 Assert.True(filter.KeyMayMatch(0, "test/231"));

			 Assert.False(filter.KeyMayMatch(0, "test/23"));

			 Assert.False(filter.KeyMayMatch(0, "test/5"));
		 }


		private MemoryMappedViewAccessor ToMemoryMappedViewAccessor(Stream stream)
		{
			var memoryMappedFile = MemoryMappedFile.CreateNew(Guid.NewGuid().ToString(), stream.Length);
			disposables.Add(memoryMappedFile);

			stream.Position = 0;
			using(var mms = memoryMappedFile.CreateViewStream(0, stream.Length))
				stream.CopyTo(mms);

			var accessor = memoryMappedFile.CreateViewAccessor(0, stream.Length);
			disposables.Add(accessor);
			return accessor;
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