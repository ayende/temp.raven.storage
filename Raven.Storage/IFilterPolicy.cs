namespace Raven.Storage
{
	public interface IFilterPolicy
	{
		IFilterBuilder CreateBuidler();
		IFilter CreateFilter();
		string Name { get; }
	}
}