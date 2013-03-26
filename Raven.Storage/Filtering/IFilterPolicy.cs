namespace Raven.Storage.Filtering
{
	public interface IFilterPolicy
	{
		IFilterBuilder CreateBuidler();
		IFilter CreateFilter();
		string Name { get; }
	}
}