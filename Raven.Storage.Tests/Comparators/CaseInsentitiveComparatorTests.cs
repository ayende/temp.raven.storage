namespace Raven.Storage.Tests.Comparators
{
	using Raven.Storage.Comparing;

	using Xunit;

	public class CaseInsensitiveComparatorTests
	{
		[Fact]
		public void ComparatorTest()
		{
			Assert.Equal(0, CaseInsensitiveComparator.Default.Compare("a", "a"));
			Assert.Equal(0, CaseInsensitiveComparator.Default.Compare("A", "A"));
			Assert.Equal(0, CaseInsensitiveComparator.Default.Compare("a", "A"));
			Assert.Equal(0, CaseInsensitiveComparator.Default.Compare("A", "a"));
			Assert.NotEqual(0, CaseInsensitiveComparator.Default.Compare("8", "A"));
			Assert.NotEqual(0, CaseInsensitiveComparator.Default.Compare("A", "8"));
			Assert.NotEqual(0, CaseInsensitiveComparator.Default.Compare("A", "B"));
			Assert.NotEqual(0, CaseInsensitiveComparator.Default.Compare("B", "A"));
			Assert.NotEqual(0, CaseInsensitiveComparator.Default.Compare("a", "b"));
			Assert.NotEqual(0, CaseInsensitiveComparator.Default.Compare("b", "a"));
			Assert.Equal(-32, CaseInsensitiveComparator.Default.Compare("!", "A"));
			Assert.Equal(32, CaseInsensitiveComparator.Default.Compare("A", "!"));
			Assert.Equal(0, CaseInsensitiveComparator.Default.Compare("ą", "ą"));
			// Ignoring this for now! 
			// Assert.Equal(0, CaseInsensitiveComparator.Default.Compare("ą", "Ą"));
		}
	}
}