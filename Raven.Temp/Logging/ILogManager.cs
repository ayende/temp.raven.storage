using System;

namespace Raven.Temp.Logging
{
	public interface ILogManager
	{
		ILog GetLogger(string name);

		IDisposable OpenNestedConext(string message);

		IDisposable OpenMappedContext(string key, string value);
	}
}