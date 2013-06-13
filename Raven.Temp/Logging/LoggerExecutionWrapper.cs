﻿using System;

namespace Raven.Abstractions.Logging
{
	public class LoggerExecutionWrapper : ILog
	{
		public const string FailedToGenerateLogMessage = "Failed to generate log message";
		private readonly ILog logger;
		private readonly string loggerName;
		private readonly Target[] targets;

		public LoggerExecutionWrapper(ILog logger, string loggerName, Target[] targets)
		{
			this.logger = logger;
			this.loggerName = loggerName;
			this.targets = targets;
		}

		public ILog WrappedLogger
		{
			get { return logger; }
		}

		#region ILog Members

		public bool IsDebugEnabled
		{
			get { return logger.IsDebugEnabled; }
		}

		public bool IsWarnEnabled
		{
			get { return logger.IsWarnEnabled; }
		}

		public void Log(LogLevel logLevel, Func<string> messageFunc)
		{
			Func<string> wrappedMessageFunc = () =>
			{
				try
				{
					return messageFunc();
				}
				catch (Exception ex)
				{
					Log(LogLevel.Error, () => FailedToGenerateLogMessage, ex);
				}
				return null;
			};
			logger.Log(logLevel, wrappedMessageFunc);

			if (ShouldLogToTargets(logLevel) == false || targets.Length == 0)
				return;
			var formattedMessage = wrappedMessageFunc();
			foreach (var target in targets)
			{
				target.Write(new LogEventInfo
				{
					Exception = null,
					FormattedMessage = formattedMessage,
					Level = logLevel,
					LoggerName = loggerName,
					TimeStamp = SystemTime.UtcNow,
				});
			}
		}

		private bool ShouldLogToTargets(LogLevel logLevel)
		{
			switch (logLevel)
			{
				case LogLevel.Debug:
				case LogLevel.Info:
					return logger.IsDebugEnabled;
				case LogLevel.Warn:
					return logger.IsWarnEnabled;
				case LogLevel.Error:
				case LogLevel.Fatal:
					return true; // errors & fatal are ALWAYS logged to registered targets
				default:
					return true;
			}
		}

		public void Log<TException>(LogLevel logLevel, Func<string> messageFunc, TException exception)
			where TException : Exception
		{
			Func<string> wrappedMessageFunc = () =>
			{
				try
				{
					return messageFunc();
				}
				catch (Exception ex)
				{
					Log(LogLevel.Error, () => FailedToGenerateLogMessage, ex);
				}
				return null;
			};
			logger.Log(logLevel, wrappedMessageFunc, exception);
			foreach (var target in targets)
			{
				target.Write(new LogEventInfo
				{
					Exception = exception,
					FormattedMessage = wrappedMessageFunc(),
					Level = logLevel,
					LoggerName = loggerName,
					TimeStamp = SystemTime.UtcNow,
				});
			}
		}

		#endregion
	}
}