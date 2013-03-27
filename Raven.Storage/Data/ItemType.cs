namespace Raven.Storage.Data
{
	public enum ItemType : byte
	{
		Deletion = 0, 
		Value = 1,
		ValueForSeek = Value,
	}
}