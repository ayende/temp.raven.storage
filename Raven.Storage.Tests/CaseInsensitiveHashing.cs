using Raven.Storage.Filtering;
using Xunit;

namespace Raven.Storage.Tests
{
	public class CaseInsensitiveHashing
	{
		[Fact]
		public void CanHashValueAndGetSameResult()
		{
			var hash1 = Bloom.HashCaseInsensitive("tESt");
			var hash2 = Bloom.HashCaseInsensitive("test");
			Assert.Equal(hash1, hash2);
		}

		[Fact]
		public void CanHashValueAndGetSameResultNotEvenlyDividedBy4()
		{
			var hash1 = Bloom.HashCaseInsensitive("tesT3");
			var hash2 = Bloom.HashCaseInsensitive("teSt3");
			Assert.Equal(hash1, hash2);
		}

		[Fact]
		public void DifferentValuesDifferentHashes()
		{
			var hash1 = Bloom.HashCaseInsensitive("test1");
			var hash2 = Bloom.HashCaseInsensitive("test2");
			Assert.NotEqual(hash1, hash2);
		}
	}
}