namespace Raven.Storage.Impl
{
	using System;
	using System.Text;

	using Raven.Storage.Data;

	public class Status
	{
		private readonly StatusCode code;

		private readonly Slice firstMessage;

		private readonly Slice secondMessage;

		private Status() : this(StatusCode.OK, string.Empty, string.Empty)
		{
		}

		private Status(StatusCode code, Slice firstMessage, Slice secondMessage)
		{
			this.code = code;
			this.firstMessage = firstMessage;
			this.secondMessage = secondMessage;
		}

		public static Status OK()
		{
			return new Status();
		}

		public static Status NotFound(Slice firstMessage, Slice secondMessage = new Slice())
		{
			return new Status(StatusCode.NotFound, firstMessage, secondMessage);
		}

		public static Status Corruption(Slice firstMessage, Slice secondMessage = new Slice())
		{
			return new Status(StatusCode.Corruption, firstMessage, secondMessage);
		}

		public static Status NotSupported(Slice firstMessage, Slice secondMessage = new Slice())
		{
			return new Status(StatusCode.NotSupported, firstMessage, secondMessage);
		}

		public static Status InvalidArgument(Slice firstMessage, Slice secondMessage = new Slice())
		{
			return new Status(StatusCode.InvalidArgument, firstMessage, secondMessage);
		}

		public static Status IOError(Slice firstMessage, Slice secondMessage = new Slice())
		{
			return new Status(StatusCode.IOError, firstMessage, secondMessage);
		}

		public bool IsOK()
		{
			return code == StatusCode.OK;
		}

		public bool IsNotFound()
		{
			return code == StatusCode.NotFound;
		}

		public bool IsCorruption()
		{
			return code == StatusCode.Corruption;
		}

		public bool IsInvalidArgument()
		{
			return code == StatusCode.InvalidArgument;
		}

		public bool IsNotSupported()
		{
			return code == StatusCode.NotSupported;
		}

		public bool IsIOError()
		{
			return code == StatusCode.IOError;
		}

		public override string ToString()
		{
			string type;
			switch (code)
			{
				case StatusCode.OK:
					return "OK";
				case StatusCode.NotFound:
					type = "NotFound";
					break;
				case StatusCode.Corruption:
					type = "Corruption";
					break;
				case StatusCode.NotSupported:
					type = "Not implemented";
					break;
				case StatusCode.InvalidArgument:
					type = "Invalid argument";
					break;
				case StatusCode.IOError:
					type = "IO error";
					break;
				default:
					throw new NotSupportedException(code.ToString());
			}

			return string.Format("{0}:{1}", type, FormatMessage());
		}

		private string FormatMessage()
		{
			var result = string.Empty;
			if (firstMessage.Count > 0)
			{
				result += Encoding.UTF8.GetString(firstMessage.Array);
			}

			if (secondMessage.Count > 0)
			{
				result += ": " + Encoding.UTF8.GetString(secondMessage.Array);
			}

			return result;
		}
	}
}