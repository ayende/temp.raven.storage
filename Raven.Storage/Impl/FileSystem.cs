using System.IO;

namespace Raven.Storage.Impl
{
	public class FileSystem
	{
		public virtual Stream NewWritable(string name)
		{
			return File.OpenWrite(name);
		}

		public Stream NewWritable(string name, int num, string ext)
		{
			return NewWritable(string.Format("{0}{1:000000}.{2}", name, num, ext));
		}
	}
}