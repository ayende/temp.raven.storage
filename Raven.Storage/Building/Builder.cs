namespace Raven.Storage.Building
{
	using Raven.Storage.Impl;

	/// <summary>
	/// A helper class so we can efficiently apply a whole sequence
	/// of edits to a particular state without creating intermediate
	/// Versions that contain full copies of the intermediate state.
	/// </summary>
	public class Builder
	{
		/// <summary>
		/// Initialize a builder with the files from *base and other info from *vset
		/// </summary>
		public Builder(VersionSet versionSet, Version @base)
		{
			throw new System.NotImplementedException();
		}

		public void Apply(VersionEdit edit)
		{
			throw new System.NotImplementedException();
		}

		public void SaveTo(Version version)
		{
			throw new System.NotImplementedException();
		}
	}
}