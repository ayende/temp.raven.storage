using System;

namespace Raven.Temp.Logging
{
	public interface ILog
	{
		bool IsDebugEnabled { get; }

		bool IsWarnEnabled { get; }

		void Log(LogLevel logLevel, Func<string> messageFunc);

		void Log<TException>(LogLevel logLevel, Func<string> messageFunc, TException exception) where TException : Exception;
	}
}