using System;
using System.Runtime.Serialization;

namespace Raven.Storage.Exceptions
{
	[Serializable]
	public class ManifestFileException : Exception
	{
		public ManifestFileException()
		{
		}

		public ManifestFileException(string message) : base(message)
		{
		}

		public ManifestFileException(string message, Exception inner) : base(message, inner)
		{
		}

		protected ManifestFileException(
			SerializationInfo info,
			StreamingContext context) : base(info, context)
		{
		}
	}
}