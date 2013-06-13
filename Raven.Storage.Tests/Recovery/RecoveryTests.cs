// -----------------------------------------------------------------------
//  <copyright file="RecoveryTests.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.IO;
using Xunit;

namespace Raven.Storage.Tests.Recovery
{
	using System.Text;

	public class RecoveryTests : StorageTestBase
	{
		[Fact]
		public async void ShouldRecoverDataFromLogFile()
		{
			var storage = await NewStorageAsync();

			var name = storage.Name;

			var str1 = "test1";
			var str2 = "test2";

			var s1 = new MemoryStream(Encoding.UTF8.GetBytes(str1));
			var s2 = new MemoryStream(Encoding.UTF8.GetBytes(str2));

			var writeBatch = new WriteBatch();
			writeBatch.Put("A", s1);
			writeBatch.Put("B", s2);
			await storage.Writer.WriteAsync(writeBatch);

			storage.Dispose();

			using (var newStorage = new Storage(name, new StorageOptions()))
			{
				await newStorage.InitAsync();
				AssertEqual(str1, newStorage.Reader.Read("A"));
				AssertEqual(str2, newStorage.Reader.Read("B"));
			}
		}

		public void AssertEqual(string expected, Stream actual)
		{
			actual.Position = 0;

			using (var reader = new StreamReader(actual))
			{
				var str2 = reader.ReadToEnd();

				Assert.Equal(expected, str2);
			}
		}
	}
}