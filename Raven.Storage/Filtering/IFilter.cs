using System;
using Raven.Storage.Data;

namespace Raven.Storage.Filtering
{
	public interface IFilter : IDisposable
	{
		bool KeyMayMatch(long position, Slice key);
	}
}