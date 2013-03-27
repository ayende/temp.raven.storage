using System;
using System.IO;

namespace Raven.Storage.Memory
{
	public interface IAccessor : IDisposable
	{
		IArrayAccessor CreateAccessor(long pos, long count);
		Stream CreateStream(long pos, long count);
	}
}