using System.Collections.Generic;
using System.IO;
using Raven.Storage.Data;

namespace Raven.Storage.Filtering
{
	public interface IFilterBuilder
	{
		void CreateFilter(List<Slice> keys, Stream output);
	}
}