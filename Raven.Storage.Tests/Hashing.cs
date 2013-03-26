using Raven.Storage.Filtering;
using Xunit;

namespace Raven.Storage.Tests
{
	public class Hashing
	{
		[Fact]
		public void CanHashValueAndGetSameResult()
		{
			var hash1 = Bloom.Hash("test");
			var hash2 = Bloom.Hash("test");
			Assert.Equal(hash1, hash2);
		}

		[Fact]
		public void CanHashValueAndGetSameResultNotEvenlyDividedBy4()
		{
			var hash1 = Bloom.Hash("test3");
			var hash2 = Bloom.Hash("test3");
			Assert.Equal(hash1, hash2);
		}

		[Fact]
		public void DifferentValuesDifferentHashes()
		{
			var hash1 = Bloom.Hash("test1");
			var hash2 = Bloom.Hash("test2");
			Assert.NotEqual(hash1, hash2);
		}
	}
}
