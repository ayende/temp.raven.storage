using System.IO;
using Raven.Storage.Data;

namespace Raven.Storage
{
	public interface IFilterBuilder
	{
		void Add(Slice key);
		void StartBlock(long pos);
		BlockHandle Finish(Stream stream);
	}
}