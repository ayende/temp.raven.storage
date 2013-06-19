using System.IO;
using System.Text;

namespace Raven.Storage.Tests.Recovery
{
	public static class DbExt
	{
		public static void Put(this Storage db, string key, string val)
		{
			var writeBatch = new WriteBatch();
			writeBatch.Put(key, new MemoryStream(Encoding.UTF8.GetBytes(val)));
			db.Writer.WriteAsync(writeBatch).Wait();
		}
	}
}