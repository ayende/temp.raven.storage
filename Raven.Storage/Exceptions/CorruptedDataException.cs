using System;
using System.Runtime.Serialization;

namespace Raven.Storage.Exceptions
{
	[Serializable]
	public class CorruptedDataException : Exception
	{
		public CorruptedDataException()
		{
		}

		public CorruptedDataException(string message) : base(message)
		{
		}

		public CorruptedDataException(string message, Exception inner) : base(message, inner)
		{
		}

		protected CorruptedDataException(
			SerializationInfo info,
			StreamingContext context) : base(info, context)
		{
		}
	}
}