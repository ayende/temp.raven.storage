namespace Raven.Storage.Exceptions
{
	using System;

	public class LogWriterException : Exception
	{
		public LogWriterException(Exception exception)
			: base(string.Empty, exception)
		{
		}
	}
}